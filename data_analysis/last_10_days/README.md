# Statistic on last 10 days

## Folder information

- `python` folder contains scripts writing in `python`, but this method is **very slow**.
- `mysql` folder contains scripts writing in `MySQL`. It's very fast and the result are stored in table `last_10_info`.

The `mysql` method is used finally.

## Table structure

Each column is detailed as follows:
- `user_id`: user identification
- `click_time`: each user's click times in the last 10 days
- `cart_time`: each user's cart times in the last 10 days
- `favor_time`: each user's favor times in the last 10 days
- `buy_time`: each user's buy times in the last 10 days

**NOTE: If the specific event (e.g. click, favor, cart, buy etc.) dosen't happen in the last 10 days, the corresponding column is set to `null`!**