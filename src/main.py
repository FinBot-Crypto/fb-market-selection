import asyncio
import logging
import os
import json
import time
import ccxt
import nats
from nats.js.errors import NotFoundError
from datetime import datetime
import pandas as pd

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fb-market-selection")

# Configurações via Ambiente
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", 3600))  # 1 hora
MIN_VOLUME_USDT = int(os.getenv("MIN_VOLUME_USDT", 10_000_000))
TOP_N = int(os.getenv("TOP_N", 20))
NATS_RECONNECT_WAIT = 5  # segundos


async def get_market_data():
    """Busca dados da Binance e filtra ativos por liquidez."""
    exchange = ccxt.binance({'enableRateLimit': True})

    try:
        logger.info("Buscando tickers da Binance...")
        tickers = await asyncio.to_thread(exchange.fetch_tickers)

        data = []
        for symbol, ticker in tickers.items():
            if not symbol.endswith('/USDT'):
                continue
            vol = ticker.get('quoteVolume')
            last = ticker.get('last')
            pct = ticker.get('percentage')
            if vol is None or last is None:
                continue
            data.append({
                'symbol': symbol,
                'last': last,
                'quoteVolume': vol,
                'percentage': pct or 0.0,
            })

        df = pd.DataFrame(data)
        if df.empty:
            logger.warning("Nenhum par USDT encontrado.")
            return []

        # Filtro de Volume mínimo
        df = df[df['quoteVolume'] >= MIN_VOLUME_USDT]

        # Top N moedas mais líquidas
        top_assets = df.sort_values(by='quoteVolume', ascending=False).head(TOP_N)

        now = datetime.utcnow().isoformat()
        selected = [
            {
                "symbol": row['symbol'],
                "volume_24h": row['quoteVolume'],
                "last_price": row['last'],
                "change_24h": row['percentage'],
                "timestamp": now,
            }
            for _, row in top_assets.iterrows()
        ]

        logger.info(f"Selecionados {len(selected)} ativos (de {len(data)} pares USDT).")
        return selected

    except Exception as e:
        logger.error(f"Erro ao buscar dados da Binance: {e}")
        return []
    finally:
        await asyncio.to_thread(exchange.close)


async def connect_nats():
    """Conecta ao NATS com retry infinito."""
    while True:
        try:
            nc = await nats.connect(NATS_URL)
            js = nc.jetstream()
            logger.info(f"Conectado ao NATS em {NATS_URL}")

            # Garantir Stream PIPELINE
            try:
                await js.find_stream_name_by_subject("market.updated")
                logger.info("Stream PIPELINE já existe.")
            except Exception:
                await js.add_stream(
                    name="PIPELINE",
                    subjects=["market.>", "strategies.>", "trade.>", "risk.>"],
                )
                logger.info("Stream PIPELINE criado.")

            # KV Stores
            kv_market = await js.key_value(bucket='market_cache')
            kv_positions = await js.key_value(bucket='active_positions')
            logger.info("KV Stores prontas.")

            return nc, js, kv_market, kv_positions

        except Exception as e:
            logger.error(f"Erro ao conectar NATS: {e} — retry em {NATS_RECONNECT_WAIT}s")
            await asyncio.sleep(NATS_RECONNECT_WAIT)


async def ensure_kv(js, bucket):
    """Cria KV bucket se não existir, senão apenas acessa."""
    try:
        return await js.create_key_value(bucket=bucket)
    except Exception:
        return await js.key_value(bucket=bucket)


async def main():
    nc, js, kv_market, kv_positions = await connect_nats()

    while True:
        start_time = time.time()

        # Reconectar se necessário
        if nc.is_closed:
            logger.warning("Conexão NATS perdida, reconectando...")
            nc, js, kv_market, kv_positions = await connect_nats()

        # 1. Buscar dados
        assets = await get_market_data()

        if assets:
            # 2. Filtrar ativos que já têm posição aberta
            filtered_assets = []
            for asset in assets:
                kv_key = asset['symbol'].replace('/', '_').replace('.', '_')
                try:
                    await kv_positions.get(kv_key)
                    logger.debug(f"Ativo {asset['symbol']} ignorado (posição ativa).")
                except NotFoundError:
                    filtered_assets.append(asset)
                except Exception as e:
                    logger.warning(f"Erro ao checar posição de {asset['symbol']}: {e}")
                    filtered_assets.append(asset)

            if filtered_assets:
                payload = json.dumps(filtered_assets).encode()

                # 3. Publicar via JetStream
                await js.publish("market.updated", payload)

                # 4. Cache para Dashboard
                await kv_market.put("top_assets", payload)

                logger.info(f"Publicado {len(filtered_assets)} ativos em 'market.updated'.")
            else:
                logger.info("Nenhum ativo novo (todos com posição ativa).")

        # Aguardar intervalo
        elapsed = time.time() - start_time
        wait_time = max(0, UPDATE_INTERVAL - elapsed)
        logger.info(f"Próxima atualização em {int(wait_time)}s...")
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    asyncio.run(main())
