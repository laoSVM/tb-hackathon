![](img/hackathon.png)

## Technical Setup

1. Create a single github account with a public repository for each team and or individual if you are not with a team
2. Install git on your local machine
3. Install docker on your locak machine (Optional)
4. Clone this repo ``git clone https://github.com/TradeBlock/tb-hackathon.git``
5. install [python version 3.10](https://www.python.org/downloads/) on your local development environment.
8. run ``pip install -r requirements.txt`` in directory you cloned
9. create .env file # for AWS credentials

## Optional Setup - Create Python Virtual Environment
1. Create the environment with ``python3.10 -m venv .venv``
2. Activate the environment before step 8 in Technical Setup with ``source .venv/bin/activate``
3. Continue with step 8 in Technical Setup

## Problem Statement 

* To be RELEASED Thurs. 2/17/22

### Summary
### Constraints
### Measures of Success

## Data

* Data will be available for bulk download in .csv files
* Data also can be easily downloaded into your program one row at a time in main.py
* Row data will be formatted as follows:
  * pair, price, amount, timestamp
  * pair: EXCHANGE-CRYPTO-usd where EXCHANGE is a specific exchange where the trade happened and CRYPTO is either bitcoin (xbt) or ethereum (eth)
  * price: price in dollars of the trade
  * amount: number of units which is either bitcoins or ethereum contracts
  * timestamp:  is seconds since epoch and can be used in python via ``datetime.datetime.fromtimestamp(TIMESTAMP)``
  * Sample rows are:
    * ``okfq-xbt-usd,14682.26,2,1514765115``
    * ``okf1-xbt-usd,13793.65,2,1514765115``
    * ``stmp-xbt-usd,13789.01,0.00152381,1514765115``

## Response Data Object Definitions

### Fill

```
{
  “price”: decimal.Decimal,       # fill price
  “volume”: decimal.Decimal,      # fill volume
  “error_code”: str               # Optional. “rejected”
  “error_msg”: str                # Optional. description of error
  “unfilled”: dict[str, Decimal]  # unfilled portion of trades (xbt,eth)
}
```

### Unfilled

```
{
  “xbt”: decimal.Decimal,         # unfilled xbt volume
  “eth”: decimal.Decimal,         # unfilled eth volume
}
```

## Errors

When an error occurs with your attempted trade, you will receive an empty fill dictionary and the
unfilled trades dictionary—this may be caused by:
* Insufficient funds
* Yielded a type other than Trade or None; or
* Invalid base currency in yielded Trade

## References

* [bitcoin](https://bitcoin.org/bitcoin.pdf)
* [ethereum](https://ethereum.org/en/whitepaper/)
