# covid

Exemplo de uso:

```$ python3 -i api.py
>>> res = API.get(date=all, city="Rio de Janeiro-RJ")
>>> API.to_csv('rio', res) # rio.csv
Tempo: 29.331178s
>>> res = API.get(date=all, city="Niteroi-RJ")
>>> API.to_csv('niteroi', res) # niteroi.csv
Tempo: 29.301651s
>>> res = API.get(date=all, city={"Niteroi-RJ", "Rio de Janeiro-RJ"})
>>> API.to_csv('rio+niteroi', res) # rio+niteroi.csv
Tempo: 52.293059s
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
2. `all` : 
3. `str` : Se `city` for `None`, retorna dados a nível estadual. Se `city` for `all`, retorna todas as cidades deste estado.
4. `set` contendo __strings__ : retorna os resultados como descritos acima, mas para diversos estados.

## Cidades (`city`):
1. `None`(default) : Se `state` também for `None`, retorna dados a nível federal. Caso contrário, retorna dados a nível estadual.
2. `all` : retorna dados para todas as cidades, sob o escopo definido por `state`.
3. `str` : __string__ no formato "Nome da Cidade-UF".
4. `set` contendo __strings__ :retorna os resultados como descritos acima, mas para diversas cidades.

*Nota: algumas dessas funcionalidades ainda não foram implementadas, mas as dos exemplos já funcionam.*

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
| `str`            | `str`         | **Ø**              |
| `set`            | `str`         | **Ø**              |
| `None`           | `set`         | Municipal          |
| `all`            | `set`         | **Ø**              |
| `str`            | `set`         | **Ø**              |
| `set`            | `set`         | **Ø**              |
