# 数据清洗结果说明

所有清晰结果均保存到`washed`数据库中，原有数据库`tianchi`中数据均保持不变。

## 表格说明

- item_subset
    + 商品子集表
    + 所有的item_id
- tianchi_p
    + 过滤掉对非商品子集内商品的操作记录
- user_click_10
    + 最后10天点击数
- user_favor_10
    + 最后10天收藏数
- user_cart_10
    + 最后10天加购物车数
- user_buy_10
    + 最后10天购买数
- user_action_10
    + 最后10天行为总表
- tianchi_p_10
    + 过滤掉最后10天没有任何操作的人
- user_buy
    + 选出用户在所有天的购买次数
- user_click
    + 选出用户在所天的点击次数
- tianchi_p_10_nobuy
    + 过滤掉从未购买过的用户记录