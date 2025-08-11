from rapidfuzz import process, fuzz
import pandas as pd

mapa_meses = {
    "janeiro": 1, "jan": 1, "1": 1, "01": 1,
    "fevereiro": 2, "feb": 2, "2": 2, "02": 2,
    "março": 3, "mar": 3, "3": 3, "03": 3,
    "abril": 4, "apr": 4, "4": 4, "04": 4,
    "maio": 5, "may": 5, "5": 5, "05": 5,
    "junho": 6, "jun": 6, "6": 6, "06": 6,
    "julho": 7, "jul": 7, "7": 7, "07": 7,
    "agosto": 8, "aug": 8, "8": 8, "08": 8,
    "setembro": 9, "sep": 9, "9": 9, "09": 9,
    "outubro": 10, "oct": 10, "10": 10,
    "novembro": 11, "nov": 11, "11": 11,
    "dezembro": 12, "dec": 12, "12": 12
}
meses_pt = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
]
def carregar_dados():
    df_vendas = pd.read_excel("ObjetosTeca.xlsx", sheet_name="Base")
    df_skus = pd.read_excel("ObjetosTeca.xlsx", sheet_name="SKUS")
    return df_vendas, df_skus

def preprocessar_nomes(df_skus):
    nomes_corretos = [str(nome).lower().strip() for nome in df_skus["Nome"].tolist()]
    nomes_originais = df_skus["Nome"].tolist()
    return nomes_corretos, nomes_originais

def corrigir_nomes(df_vendas, nomes_corretos, nomes_originais):
    def encontrar_melhor_nome(nome):
        nome = str(nome).lower().strip()
        match, score, _ = process.extractOne(nome, nomes_corretos, scorer=fuzz.token_set_ratio)
        return nomes_originais[nomes_corretos.index(match)] if score >= 70 else nome
    df_vendas["Produto_Corrigido"] = df_vendas["Objeto"].apply(encontrar_melhor_nome)
    df_vendas.drop(columns=["Objeto"], inplace=True)
    return df_vendas

def formatar_valores_numericos(df_vendas):
    colunas_decimais = ["Investido", "Receita", "ROAS", "Ticket médio"]
    for coluna in colunas_decimais:
        df_vendas[coluna] = pd.to_numeric(df_vendas[coluna], errors='coerce').round(2)
        df_vendas[coluna] = df_vendas[coluna].apply(lambda x: int(x) if x == x // 1 else x)
    return df_vendas

def tratar_data(df_vendas):
    df_vendas["Mês"] = df_vendas["Mês"].astype(str).str.lower().str.strip()
    df_vendas["Mês"] = df_vendas["Mês"].map(mapa_meses).fillna(1).astype(int)
    df_vendas["Ano"] = pd.to_numeric(df_vendas["Ano"], errors="coerce")
    df_vendas.loc[df_vendas["Ano"] != 2022, "Ano"] = 2022
    def corrigir_data(row):
        data_str = str(row["Data"]).replace("-", "/").strip().lower()

        if "not a date" in data_str:
            return pd.to_datetime(f"01/12/2022", dayfirst=True)
        
        if data_str == "00/00/0000":
            return pd.to_datetime(f"01/10/2022", dayfirst=True)
        
        if data_str == "32/13/2022":
            return pd.to_datetime(f"01/06/2022", dayfirst=True)
        
        if data_str in ["31/04/2022", "31/4/2022"]:
            return pd.to_datetime(f"01/12/2022", dayfirst=True)
        data = pd.to_datetime(data_str, dayfirst=True, errors="coerce")

        if not pd.isna(data) and 2000 <= data.year <= 2100:
            return data
        return pd.to_datetime(f"01/{row['Mês']}/{row['Ano']}", dayfirst=True, errors="coerce")
    
    df_vendas["Data"] = df_vendas.apply(corrigir_data, axis=1)
    df_vendas["Data"] = df_vendas["Data"].dt.strftime("%d/%m/%Y")
    df_vendas.drop(columns=["Mês", "Ano"], inplace=True)
    return df_vendas

def formatar_mes_ano(data):
    return f"{meses_pt[data.month - 1]} {data.year}"

def mostrar_resultados_por_mes(df, grupo_col, valor_col, titulo, tipo="maior"):
    print(f"\n{titulo}")
    print("="*50)
    df['Data_Ord'] = pd.to_datetime(df[grupo_col])
    df = df.sort_values('Data_Ord')
    for mes_ano, group in df.groupby(grupo_col):
        data = pd.to_datetime(mes_ano)
        mes_formatado = formatar_mes_ano(data)
        if tipo == "maior":
            ranking = group.sort_values(valor_col, ascending=False).head(5)
        else:
            ranking = group.sort_values(valor_col, ascending=True).head(5)
        print(f"\n{mes_formatado}:")
        for i, (_, row) in enumerate(ranking.iterrows(), 1):
            valor_formatado = f"{row[valor_col]:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            print(f"{i}. {row['Produto_Corrigido']} - {valor_formatado}")

def salvar_planilha_tratada(df_vendas, df_skus):
    with pd.ExcelWriter("Planilha Tratada.xlsx", engine='openpyxl') as writer:
        df_vendas.to_excel(writer, sheet_name='Base Tratada', index=False)
        df_skus.to_excel(writer, sheet_name='SKUS Original', index=False)

def remover_data_duplicada(df_vendas):
    df_vendas = df_vendas.drop(columns=["Mês_Ano"])
    return df_vendas

def main():
    df_vendas, df_skus = carregar_dados()
    df_vendas = tratar_data(df_vendas)
    nomes_corretos, nomes_originais = preprocessar_nomes(df_skus)
    df_vendas = corrigir_nomes(df_vendas, nomes_corretos, nomes_originais)
    df_vendas = formatar_valores_numericos(df_vendas)
    df_vendas["Data"] = pd.to_datetime(df_vendas["Data"], format= "%d/%m/%Y")  
    df_vendas["Mês_Ano"] = df_vendas["Data"].dt.to_period('M').dt.to_timestamp()
    df_vendas = df_vendas.sort_values("Data")
    
    faturamento_mensal = df_vendas.groupby(["Mês_Ano", "Produto_Corrigido"])["Receita"].sum().reset_index()
    mostrar_resultados_por_mes(faturamento_mensal, "Mês_Ano", "Receita", 
                             "1. TOP 5 PRODUTOS POR FATURAMENTO MENSAL", "maior")
    
    if "Cliques" in df_vendas.columns:
        cliques_mensal = df_vendas.groupby(["Mês_Ano", "Produto_Corrigido"])["Cliques"].sum().reset_index()
        mostrar_resultados_por_mes(cliques_mensal, "Mês_Ano", "Cliques",
                                 "2. TOP 5 PRODUTOS COM MENOS CLIQUES POR MÊS", "menor")
    else:
        print("\nAVISO: Coluna 'Cliques' não encontrada - análise não realizada")

    if "Ticket médio" in df_vendas.columns:
        print("\n3. TOP 5 PRODUTOS COM MAIOR TICKET MÉDIO NO ANO")
        print("="*50)
        ticket_medio = df_vendas.groupby("Produto_Corrigido")["Ticket médio"].mean().reset_index()
        top5_ticket = ticket_medio.sort_values("Ticket médio", ascending=False).head(5)
        
        for i, (_, row) in enumerate(top5_ticket.iterrows(), 1):
            valor_formatado = f"{row['Ticket médio']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            print(f"{i}. {row['Produto_Corrigido']} - R$ {valor_formatado}")
    
    print("\n4. INSIGHTS ADICIONAIS")
    print("="*50)
    print("Insight Positivo:")
    print("O maior pico de vendas ocorre em novembro, no mês de Black Friday, com um aumento significativo no faturamento.\n")
    print("Insight Negativo:")
    print("Bicicleta vem sendo disparado o produto dando o maior prejuízo, total investido R$280.374,72 e receita de R$0,00.")

    df_vendas = remover_data_duplicada(df_vendas)
    df_vendas["Data"] = df_vendas["Data"].dt.strftime("%d/%m/%Y")
    salvar_planilha_tratada(df_vendas, df_skus)
if __name__ == "__main__":
    main()