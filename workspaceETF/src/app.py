import yfinance as yf
import pandas as pd
from datetime import datetime
import duckdb
import hashlib
import logging
import plotly.express as px
import pandas as pd

# Configuração do logging
logging.basicConfig(filename='etf_data_operations.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s')

# Lista de ETFs
etf_symbols = ["SPHD", "TFLO", "JEPI","VOO","QQQM","SCHD","DHS","SHY","TLT"]

# DataFrame para consolidar os dados de todos os ETFs
final_data = pd.DataFrame()

for etf_symbol in etf_symbols:
    # Baixar informações do ETF
    etf_data = yf.Ticker(etf_symbol)
    #print(f"Coletando dados para o ETF: {etf_symbol}")
   
    # Obter dados históricos (último ano)
    historical_data = etf_data.history(period="1y")[["Open", "Close", "Dividends"]]
   
    # Calcular a diferença entre Open e Close
    historical_data["Difference"] = historical_data["Close"] - historical_data["Open"]
    historical_data["Ganho_x_Perda"] = historical_data["Difference"].apply(lambda x: "Ganho" if x > 0 else "Perda")
   
    # Adicionar o símbolo do ETF
    historical_data["ETF"] = etf_symbol
   
    # Obter o último valor de dividendos
    last_dividend_date = historical_data.index.max()  # Última data
    last_dividend_value = historical_data.loc[last_dividend_date, "Dividends"]
    #print(f"Último dividendo para {etf_symbol}: Data: {last_dividend_date}, Valor: {last_dividend_value}\n")
   
    # Criar a coluna hash
    historical_data['hash'] = historical_data.apply(lambda row: hashlib.md5(f"{row.name}{row['ETF']}".encode()).hexdigest(), axis=1)
   
    # Consolidar os dados no DataFrame final
    final_data = pd.concat([final_data, historical_data])

# Conectar ao DuckDB (banco de dados em memória ou arquivo)
con = duckdb.connect("etfs_database.db")

# Criar a tabela no DuckDB se não existir
con.execute("""
CREATE TABLE IF NOT EXISTS etf_data (
    date DATE,
    Open DOUBLE,
    Close DOUBLE,
    Dividends DOUBLE,
    Ganho_x_Perda STRING,
    ETF STRING,
    hash STRING PRIMARY KEY
);
""")

def geraGraficETF():
    # Converter o índice para datetime (caso não esteja)
    final_data.index = pd.to_datetime(final_data.index)

    # Gráfico de linhas para os valores de fechamento (Close)
    fig = px.line(
        final_data,
        x=final_data.index,
        y="Close",
        color="ETF",
        title="Valores de Fechamento dos ETFs",
        labels={"Close": "Preço de Fechamento (USD)", "index": "Data", "ETF": "ETF"},
        markers=True
    )

    # Adicionar dividendos como um trace separado (opcional)
    dividend_data = final_data[final_data["Dividends"] > 0]
    fig.add_scatter(
        x=dividend_data.index,
        y=dividend_data["Dividends"],
        mode="markers",
        name="Dividendos",
        marker=dict(size=10, symbol="circle", color="gold"),
        hovertemplate="Data: %{x}<br>Dividendos: %{y}<extra></extra>"
    )

    # Ajustar layout do gráfico
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="Preço de Fechamento (USD)",
        legend_title="Legenda",
        template="plotly_dark"
    )

    # Exibir o gráfico
    fig.show()

def geraGraficoBarra():
    # Encontrar o primeiro e último valor de fechamento para cada ETF
    first_last_data = final_data.groupby("ETF").agg(
        Primeiro_Fecha=("Close", "first"),
        Ultimo_Fecha=("Close", "last")
    ).reset_index()

    # Reformatar os dados para o gráfico de barras
    melted_data = first_last_data.melt(
        id_vars=["ETF"],
        value_vars=["Primeiro_Fecha", "Ultimo_Fecha"],
        var_name="Tipo",
        value_name="Valor"
    )

    # Criar o gráfico de barras
    fig = px.bar(
        melted_data,
        x="ETF",
        y="Valor",
        color="Tipo",
        barmode="group",
        text="Valor",
        title="Comparação do Valor Inicial e Atual dos ETFs",
        labels={"Valor": "Preço de Fechamento (USD)", "ETF": "ETF"},
        color_discrete_map={"Primeiro_Fecha": "green", "Ultimo_Fecha": "blue"}  # Cores personalizadas
    )

    # Ajustar o texto e o layout
    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    fig.update_layout(
        xaxis_title="ETF",
        yaxis_title="Preço de Fechamento (USD)",
        legend_title="Tipo de Valor",
        template="plotly_dark"
    )

    fig.show()

   
# Função para upsert no DuckDB com log
def upsert_etf_data(df):
    for _, row in df.iterrows():
        # Verificar se o hash já existe na tabela
        result = con.execute("""
            SELECT COUNT(*) FROM etf_data WHERE hash = ?
        """, (row['hash'],)).fetchone()
       
        if result[0] > 0:
            # Se o hash existir, verificar se o valor é diferente
            existing_data = con.execute("""
                SELECT * FROM etf_data WHERE hash = ?
            """, (row['hash'],)).fetchone()
           
            # Se o valor de 'Close' for diferente, fazer o update
            if existing_data and existing_data[2] != row['Close']:
                con.execute("""
                    UPDATE etf_data
                    SET date = ?, Open = ?, Close = ?, Dividends = ?, Ganho_x_Perda = ?, ETF = ?
                    WHERE hash = ?
                """, (row.name, row['Open'], row['Close'], row['Dividends'], row['Ganho_x_Perda'], row['ETF'], row['hash']))
                logging.info(f"Atualizando dados para o hash {row['hash']} (ETF: {row['ETF']}, Data: {row.name})")
        else:
            # Se o hash não existir, fazer o insert
            con.execute("""
                INSERT INTO etf_data (date, Open, Close, Dividends, Ganho_x_Perda, ETF, hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row.name, row['Open'], row['Close'], row['Dividends'], row['Ganho_x_Perda'], row['ETF'], row['hash']))
            logging.info(f"Inserindo dados para o hash {row['hash']} (ETF: {row['ETF']}, Data: {row.name})")

# Realizar o upsert para os dados consolidados
upsert_etf_data(final_data)

# Salvar os dados no arquivo CSV, se necessário
current_date = datetime.now().strftime("%Y-%m-%d")
final_data.to_csv(f"etfs_data_{current_date}.csv")

# Fechar a conexão com o banco de dados DuckDB
con.close()

# Mostrar os dados consolidados
print("\nDados consolidados dos ETFs:")
#geraGraficETF()
geraGraficoBarra()
#print(final_data)