import pandas as pd
import numpy as np
import re
from pathlib import Path

# ==========================
# Config / Leitura de dados
# ==========================

# Pega o diretório do arquivo .py em execução
BASE_DIR = Path(__file__).resolve().parent
PATHGLOBAL = BASE_DIR / "usuarios&contas.csv"

# leitura
BD1 = pd.read_csv(PATHGLOBAL, sep=';', dtype=str, keep_default_na=False)

# Garante colunas mínimas e tipos para evitar quebras
def _ensure_columns(df: pd.DataFrame):
    defaults = {
        'Usuario': '', 'CPF': '', 'Senha': '',
        'Saldo': 0.0,
        'Limite': '500',              # limite por saque (exemplo simples)
        'Numero_Saques': '0',
        'Limite_Saques': '3',         # até 3 saques no "mês"
        'ExtratoMensal': '',
        'Agencia': '0001',
        'Conta': ''                   # número sequencial
    }
    for k, v in defaults.items():
        if k not in df.columns:
            df[k] = v
    # Tipos
    df['Saldo'] = pd.to_numeric(df['Saldo'], errors='coerce').fillna(0.0)
    for c in ['Limite', 'Numero_Saques', 'Limite_Saques', 'Conta']:
        df[c] = df[c].astype('string')

_ensure_columns(BD1)

# ==========================
# Normalização
# ==========================
def _only_digits(s: str) -> str:
    return re.sub(r'\D+', '', s or '')

BD1['CPF']   = BD1['CPF'].astype('string').map(_only_digits).str.zfill(11)
BD1['Senha'] = BD1['Senha'].astype('string').str.strip()

# ==========================
# Assinaturas exigidas no desafio
# deposito: POSICIONAL-ONLY
# saque:   KEYWORD-ONLY
# extrato: saldo posicional + extrato keyword-only
# ==========================
def deposito(saldo, valor, extrato, /):
    """Depósito (positional-only). Retorna saldo, extrato."""
    if valor > 0:
        saldo += valor
        extrato = (extrato + f"\nDepósito: R$ {valor:.2f}") if extrato else f"Depósito: R$ {valor:.2f}"
        print(f"Depósito de R$ {valor:.2f} realizado com sucesso.")
    else:
        print("Operação falhou! O valor informado é inválido.")
    return saldo, extrato

def saque(*, saldo, valor, extrato, limite, numero_saques, limite_saques):
    """
    Saque (keyword-only). Retorna saldo, extrato, numero_saques.
    Regras simples: valor>0, valor<=saldo, valor<=limite e numero_saques<limite_saques.
    """
    if valor <= 0:
        print("Operação falhou! Valor inválido.")
        return saldo, extrato, numero_saques

    if valor > saldo:
        print("Operação falhou! Saldo insuficiente.")
        return saldo, extrato, numero_saques

    if valor > limite:
        print(f"Operação falhou! Limite por saque é R$ {limite:.2f}.")
        return saldo, extrato, numero_saques

    if numero_saques >= limite_saques:
        print("Operação falhou! Número máximo de saques atingido.")
        return saldo, extrato, numero_saques

    saldo -= valor
    numero_saques += 1
    extrato = (extrato + f"\nSaque:    R$ {valor:.2f}") if extrato else f"Saque:    R$ {valor:.2f}"
    print(f"Saque de R$ {valor:.2f} realizado com sucesso.")
    return saldo, extrato, numero_saques

def extrato(saldo, /, *, extrato):
    """Extrato: saldo posicional, extrato keyword-only (imprime)."""
    print("\n================ EXTRATO =================")
    print("Não foram realizadas movimentações." if not extrato else extrato)
    print(f"\nSaldo atual: R$ {saldo:.2f}")
    print("==========================================")

# ==========================
# “Camada” de adaptação p/ DataFrame
# ==========================
def _df_deposito(DATA: pd.DataFrame, idx: int, valor: float):
    saldo_atual = float(DATA.at[idx, 'Saldo'])
    extrato_atual = DATA.at[idx, 'ExtratoMensal']
    saldo_novo, extrato_novo = deposito(saldo_atual, valor, extrato_atual)
    DATA.at[idx, 'Saldo'] = saldo_novo
    DATA.at[idx, 'ExtratoMensal'] = extrato_novo

def _df_saque(DATA: pd.DataFrame, idx: int, valor: float):
    saldo_atual = float(DATA.at[idx, 'Saldo'])
    extrato_atual = DATA.at[idx, 'ExtratoMensal']
    limite = float(pd.to_numeric(DATA.at[idx, 'Limite'], errors='coerce') or 0)
    numero_saques = int(pd.to_numeric(DATA.at[idx, 'Numero_Saques'], errors='coerce') or 0)
    limite_saques = int(pd.to_numeric(DATA.at[idx, 'Limite_Saques'], errors='coerce') or 0)

    saldo_novo, extrato_novo, numero_saques_novo = saque(
        saldo=saldo_atual,
        valor=valor,
        extrato=extrato_atual,
        limite=limite,
        numero_saques=numero_saques,
        limite_saques=limite_saques
    )
    DATA.at[idx, 'Saldo'] = saldo_novo
    DATA.at[idx, 'ExtratoMensal'] = extrato_novo
    DATA.at[idx, 'Numero_Saques'] = str(numero_saques_novo)

def _df_extrato(DATA: pd.DataFrame, idx: int):
    extrato(float(DATA.at[idx, 'Saldo']), extrato=DATA.at[idx, 'ExtratoMensal'])

# ==========================
# Busca de usuário
# ==========================
def search_user(DATA: pd.DataFrame, CPF: str, SENHA: str):
    cpf_norm = _only_digits(str(CPF)).zfill(11)
    senha_norm = str(SENHA).strip()
    mask = (DATA['CPF'] == cpf_norm) & (DATA['Senha'] == senha_norm)
    idx = np.where(mask)[0]
    return int(idx[0]) if len(idx) else None

# ==========================
# Criar Usuário / Criar Conta
# (simples, direto; persiste no mesmo CSV)
# ==========================
def _next_account_number(DATA: pd.DataFrame) -> str:
    """Gera número sequencial de conta (string)."""
    if 'Conta' not in DATA.columns or DATA['Conta'].eq('').all():
        return '1'
    nums = pd.to_numeric(DATA['Conta'].replace('', np.nan), errors='coerce').dropna()
    return str(int(nums.max()) + 1) if len(nums) else '1'

def criar_usuario():
    """
    Cadastra um usuário (nome, nascimento, cpf, endereço, senha).
    Evita CPF duplicado e normaliza CPF (11 dígitos). Persiste no CSV.
    """
    global BD1
    nome = input("Nome completo: ").strip()
    nasc = input("Data de nascimento (DD/MM/AAAA): ").strip()
    cpf  = _only_digits(input("CPF (somente números ou com máscara): ")).zfill(11)
    end  = input("Endereço (logradouro, nro – bairro – cidade/UF): ").strip()
    senha = input("Defina uma senha: ").strip()

    if (BD1['CPF'] == cpf).any():
        print("Já existe usuário com esse CPF.")
        return

    nova_linha = {
        'Usuario': nome,
        'CPF': cpf,
        'Senha': senha,
        'Saldo': 0.0,
        'Limite': '500',
        'Numero_Saques': '0',
        'Limite_Saques': '3',
        'ExtratoMensal': '',
        'Agencia': '0001',
        'Conta': ''  # atribuímos quando criar a conta
    }
    BD1 = pd.concat([BD1, pd.DataFrame([nova_linha])], ignore_index=True)
    BD1.to_csv(PATHGLOBAL, sep=';', index=False)
    print("Usuário criado com sucesso!")

def criar_conta():
    """
    Cria conta corrente: agência fixa '0001', número sequencial.
    Vincula por CPF (pode ter várias contas). Persiste no CSV.
    """
    global BD1
    cpf = _only_digits(input("Informe o CPF do titular: ")).zfill(11)

    # Filtra todas as linhas do titular (pode ter mais de 1 conta futuramente)
    linhas_titular = BD1.index[BD1['CPF'] == cpf].tolist()
    if not linhas_titular:
        print("CPF não encontrado. Cadastre o usuário primeiro.")
        return

    num_conta = _next_account_number(BD1)
    agencia = '0001'

    # Regra simples: duplicamos a última linha do titular criando uma nova "conta"
    base_idx = linhas_titular[-1]
    nova = BD1.loc[base_idx].copy()
    nova['Conta'] = num_conta
    nova['Agencia'] = agencia
    # Zera saldo/extrato/numero_saques para a nova conta
    nova['Saldo'] = 0.0
    nova['ExtratoMensal'] = ''
    nova['Numero_Saques'] = '0'

    BD1 = pd.concat([BD1, pd.DataFrame([nova])], ignore_index=True)
    BD1.to_csv(PATHGLOBAL, sep=';', index=False)
    print(f"Conta criada com sucesso! Agência {agencia} Conta {num_conta}")

# ==========================
# Loop principal (login + operações)
# ==========================
menucrud = """

[1] Login
[2] Criar Usuário
[3] Criar Conta
[0] Sair

=> """

while True:
    opcaocrud = input(menucrud).strip()
    if opcaocrud == "1":
        cpf = input("Digite o CPF: ").strip()
        senha = input("Digite a senha do usuário: ").strip()
        user_index = search_user(BD1, cpf, senha)
        if user_index is not None:
            while True:
                print(f"\nBem-vindo, {BD1.loc[user_index, 'Usuario']}!  Agência {BD1.loc[user_index, 'Agencia']} Conta {BD1.loc[user_index, 'Conta'] or '(sem conta)'}")
                menu = """
[d] Depositar
[s] Sacar
[e] Extrato/Saldo
[q] Sair

=> """
                opcao = input(menu).strip().lower()
                if opcao == "d":
                    valor = float(input("Informe o valor do depósito: ").replace(',', '.'))
                    _df_deposito(BD1, user_index, valor)
                    BD1.to_csv(PATHGLOBAL, sep=';', index=False)

                elif opcao == "s":
                    valor = float(input("Informe o valor do saque: ").replace(',', '.'))
                    _df_saque(BD1, user_index, valor)
                    BD1.to_csv(PATHGLOBAL, sep=';', index=False)

                elif opcao == "e":
                    _df_extrato(BD1, user_index)

                elif opcao == "q":
                    print("Saindo do sistema...")
                    break
                else:
                    print("Opção inválida.")
        else:
            print("Usuário ou senha inválidos. Tente novamente.")

    elif opcaocrud == "2":
        criar_usuario()

    elif opcaocrud == "3":
        criar_conta()

    elif opcaocrud == "0":
        print("Saindo do sistema...")
        break

    else:
        print("Opção inválida.")

