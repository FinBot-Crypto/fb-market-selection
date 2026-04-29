# 🔎 fb-market-selection

Este serviço atua como os **olhos** do ecossistema FinBot. Sua função principal é varrer o mercado em busca das melhores oportunidades de liquidez, garantindo que o bot foque apenas em ativos que possuam volume real para execução.

## 🎯 Objetivos
- Monitorar todos os pares USDT na Binance.
- Filtrar ativos por liquidez (Volume 24h > 10M USDT).
- Selecionar os top 20 ativos mais relevantes para análise.
- **Evitar Overtrading**: Ignorar moedas que já possuem ordens abertas no sistema.

## ⚙️ Funcionalidade no Projeto
O `market-selection` é o gatilho inicial do pipeline. Ele roda periodicamente (padrão: 1 hora) e publica a lista de ativos selecionados no **NATS JetStream**, que é consumida pelo `fb-strategy-ml`.

### Fluxo de Dados:
1. Busca tickers via API da Binance.
2. Aplica filtros de volume e liquidez.
3. Consulta o **NATS KV Store** (`active_positions`) para excluir moedas já em operação.
4. Publica o evento `market.updated` no stream `PIPELINE`.
5. Atualiza o KV Store `market_cache` com o estado atual do mercado para o Dashboard.

## 🚀 Tecnologias
- **Python 3.11+**
- **NATS JetStream** (Mensageria e KV Store)
- **CCXT** (Integração com Binance)
- **Pandas** (Processamento de dados)

## 🐳 Docker
O serviço é containerizado e configurado para rodar em redes Docker externas, conectando-se ao container `crypto-nats`.

---
*Parte do ecossistema [FinBot-Crypto](https://github.com/FinBot-Crypto)*
