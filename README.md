# Interface para a API dos cartórios.

## **Esta documentação está um tanto quanto desatualizada. O programa está em processo de adequação as mudanças feitas na API dos cartórios.**

## Instalação
Requer Python 3.7.3 ou superior.

### Instalação Básica
```
$ git clone https://github.com/pedromxavier/covid
$ cd covid/
```

### Instalação Completa
```
$ pip -r install requirements.txt
```
**Nota**: necessário para realizar requisições assíncronas.

## Métodos:
### `API.get(date=None, state=None, city)`
#### (`cumulative`)
1. `True`: Retorna o valor acumulado de óbitos, a partir da data inicial.
2. `False`: Retorna o número de novos óbitos para cada dia.

#### Datas (`date`):
As opções para o parâmetro `date` da função `API.get` são:
1. `None` (default) : retorna resultados para o dia de hoje
2. `all` : retorna resultados para todas as datas desde 01/01
3. _string_ (`str`) no formarto ISO "AAAA-mm-dd" ou objeto `datetime.date` : retorna resultados para o dia especificado
4. Duas datas no formato acima, em uma tupla : retorna resultados entre as duas datas (incluindo início e fim).

#### Estados (`state`):
1. `None`(default) : Se `city` também for `None`, retorna dados a nível federal.
2. `all` : Se `city` for `None`, retorna dados a nível estadual. Se `city` for `all`, retorna resultados a nível municipal para todas as cidades de todos os estados.
3. `str` : Se `city` for `None`, retorna dados a nível estadual. Se `city` for `all`, retorna resultados a nível municipal para todas as cidades deste estado.
4. `set` contendo _strings_ : retorna os resultados como descritos acima, mas para diversos estados.

#### Cidades (`city`):
1. `None`(default) : Se `state` também for `None`, retorna dados a nível federal. Caso contrário, retorna dados a nível estadual.
2. `all` : retorna dados para todas as cidades, sob o escopo definido por `state`.
3. `str` : _string_ no formato "Nome da Cidade-UF".
4. `set` contendo _strings_ : retorna os resultados como descritos acima, mas para diversas cidades.

### Resumo:
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

### `API.union`