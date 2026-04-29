import asyncio
import logging
import os
import json
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
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")

async def get_market_data():
    """Busca dados da Binance e filtra ativos."""
    # Inicializa exchange (apenas leitura para market selection)
    exchange = ccxt.binance({
        'apiKey': BINANCE_API_KEY,
        'enableRateLimit': True,
    })
    
    try:
        logger.info("Buscando tickers da Binance...")
        tickers = await asyncio.to_thread(exchange.fetch_tickers)
        
        data = []
        for symbol, ticker in tickers.items():
            if symbol.endswith('/USDT'):
                data.append({
                    'symbol': symbol,
                    'last': ticker['last'],
                    'quoteVolume': ticker['quoteVolume'],
                    'percentage': ticker['percentage']
                })
        
        df = pd.DataFrame(data)
        if df.empty:
            return []

        # Filtro de Volume mínimo (10M USDT)
        min_volume = 10_000_000
        df = df[df['quoteVolume'] >= min_volume]
        
        # Top 20 moedas mais líquidas
        top_assets = df.sort_values(by='quoteVolume', ascending=False).head(20)
        
        selected = []
        for _, row in top_assets.iterrows():
            selected.append({
                "symbol": row['symbol'],
                "volume_24h": row['quoteVolume'],
                "last_price": row['last'],
                "change_24h": row['percentage'],
                "timestamp": datetime.now().isoformat()
            })
            
        return selected
    except Exception as e:
        logger.error(f"Erro ao buscar dados da Binance: {e}")
        return []
    finally:
        await asyncio.to_thread(exchange.close)

async def main():
    # Conexão NATS
    try:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        logger.info(f"Conectado ao NATS em {NATS_URL}")
        
        # Garantir que o Stream existe
        try:
            await js.add_stream(name="PIPELINE", subjects=["market.updated", "strategies.evaluated", "trade.decided"])
            logger.info("Stream PIPELINE garantido.")
        except Exception:
            logger.info("Stream PIPELINE já existe ou erro na criação.")

        # Acessar ou Criar KV Store para cache e posições
        try:
            kv_market = await js.create_key_value(bucket='market_cache')
            kv_positions = await js.create_key_value(bucket='active_positions')
        except Exception as e:
            kv_market = await js.key_value(bucket='market_cache')
            kv_positions = await js.key_value(bucket='active_positions')
            logger.info("KV Stores acessadas.")

    except Exception as e:
        logger.error(f"Erro ao conectar no NATS: {e}")
        return

    while True:
        start_time = time.time()
        
        # 1. Buscar dados
        assets = await get_market_data()
        
        if assets:
            # 2. Filtrar ativos que já têm posição aberta
            filtered_assets = []
            for asset in assets:
                try:
                    # No NATS KV, chaves não podem ter '/' ou '.'
                    kv_key = asset['symbol'].replace('/', '_').replace('.', '_')
                    await kv_positions.get(kv_key)
                    logger.info(f"Ativo {asset['symbol']} ignorado (posição já ativa).")
                except NotFoundError:
                    filtered_assets.append(asset)
            
            if filtered_assets:
                payload = json.dumps(filtered_assets).encode()
                
                # 3. Publicar via JetStream (PIPELINE)
                await js.publish("market.updated", payload)
                
                # 4. Salvar no Cache KV para o Dashboard
                await kv_market.put("top_assets", payload)
                
                logger.info(f"Publicado {len(filtered_assets)} ativos em 'market.updated'.")
            else:
                logger.info("Nenhum ativo novo após filtragem de posições ativas.")
        
        # Aguardar intervalo
        elapsed = time.time() - start_time
        wait_time = max(0, UPDATE_INTERVAL - elapsed)
        logger.info(f"Aguardando {int(wait_time)}s para próxima atualização...")
        await asyncio.sleep(wait_time)

if __name__ == "__main__":
    import time
    asyncio.run(main())
