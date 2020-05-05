# Interface para a API dos cartórios.

## Instalação

```
$ git clone https://github.com/pedromxavier/covid
$ cd covid/
$ pip -r install requirements.txt
```

## Uso

```
$ python3
>>> from api import API
>>> res = API.get(date=all, state='RJ') # dados a nível estadual para o Rio de Janeiro
Total de requisições: 126
Progresso: [===============>] 126/126      
Tempo: 6.09s
>>> API.to_csv('RJ', res)
Tempo: 0.01s
>>> res = API.get(date=all, state='RJ', city=all) # dados a nível municipal para as cidades do Rio
Total de requisições: 11718
Progresso: [===============>] 11718/11718      
Tempo: 324.16s
>>> API.to_csv('cidades-RJ', res)
Tempo: 0.15s
```

## Datas (`date`):
As opções para o parâmetro `date` da função `API.get` são:
1. `None` (default) : retorna resultados para o dia de hoje
2. `all` : retorna resultados para todas as datas desde 01/01
3. __string__ (`str`) no formarto ISO "AAAA-mm-dd" ou objeto `datetime.date` : retorna resultados para o dia especificado
4. Duas datas no formato acima, em uma tupla : retorna resultados entre as duas datas

### Exemplos:
1. `API.get(date='2020-01-01', ...) # Primeiro dia do ano`
2. `API.get(date=('2020-01-01', '2020-01-31'), ...) # Mês de janeiro`

## Estados (`state`):
1. `None`(default) : Se `city` também for `None`, retorna dados a nível federal.
2. `all` : Se `city` for `None`, retorna dados a nível estadual. Se `city` for `all`, retorna resultados a nível municipal para todas as cidades de todos os estados.
3. `str` : Se `city` for `None`, retorna dados a nível estadual. Se `city` for `all`, retorna resultados a nível municipal para todas as cidades deste estado.
4. `set` contendo __strings__ : retorna os resultados como descritos acima, mas para diversos estados.

## Cidades (`city`):
1. `None`(default) : Se `state` também for `None`, retorna dados a nível federal. Caso contrário, retorna dados a nível estadual.
2. `all` : retorna dados para todas as cidades, sob o escopo definido por `state`.
3. `str` : __string__ no formato "Nome da Cidade-UF".
4. `set` contendo __strings__ : retorna os resultados como descritos acima, mas para diversas cidades.

# Resumo:

| `state` (`None`) |`city` (`None`)| Nível do Resultado |
| ---------------- | ------------- |:------------------:|
| `None`           | `None`        | Federal            |
| `all`            | `None`        | Estadual           |
| `str`            | `None`        | Estadual           |
| `set`            | `None`        | Estadual           |
| `None`           | `all`         | **Ø**              |
| `all`            | `all`         | Municipal          |
| `str`            | `all`         | Municipal          |
| `set`            | `all`         | Municipal          |
| `None`           | `str`         | Municipal          |
| `all`            | `str`         | **Ø**              |
| `str`            | `str`         | Municipal          |
| `set`            | `str`         | **Ø**              |
| `None`           | `set`         | Municipal*         |
| `all`            | `set`         | **Ø**              |
| `str`            | `set`         | Municipal*         |
| `set`            | `set`         | **Ø**              |

*Nestes casos, aos nomes das cidades é adicionada a sigla da UF após o hífen.