## Данные api

`GET /restaurants`

```
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' \
--header 'X-Nickname: {{ your_nickname }}' \
--header 'X-Cohort: {{ your_cohort_number }}' \
--header 'X-API-KEY: {{ api_key }}'
```

{'_id': '626a81cfefa404208fe9abae', 'name': 'Кофейня №1'}
{'_id': 'a51e4e31ae4602047ec52534', 'name': 'Кубдари'}
{'_id': 'ebfa4c9b8dadfc1da37ab58d', 'name': 'PLove'}
{'_id': 'ef8c42c19b7518a9aebec106', 'name': 'Вкус Индии'}

`GET /couriers`

```
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' \
```

{'_id': '0jqno0nthckr0i9eb9b59g8', 'name': 'Эдуард Васильев'}
{'_id': '10rnbzf0afbyfpdpfayoj6l', 'name': 'Геннадий Петров'}
{'_id': '1bghie2wg0afcxt2i1k3ndc', 'name': 'Алексей Соколов'}
{'_id': '1cr998m6nfewf3310xy9a6m', 'name': 'Евгений Михайлов'}
{'_id': '1flejhply8gyubxefzwam7k', 'name': 'Елизавета Алексеева'}

`GET /deliveries`

```
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={{ restaurant_id }}&from={{ from }}&to={{ to }}&sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ limit }}' \
```

{'order_id': '63ad01954094be631265f255', 'order_ts': '2022-12-29 02:55:17.244000', 'delivery_id': '0lpt86by5d8tvugmttjpxa6', 'courier_id': 'pmft4p9r7dgl2ohd2guzd9x', 'address': 'Ул. Академика Королева, 11, кв. 174', 'delivery_ts': '2022-12-29 04:01:25.751000', 'rate': 5, 'sum': 183, 'tip_sum': 9}
{'order_id': '63ad02c495354dc976b4619c', 'order_ts': '2022-12-29 03:00:20.682000', 'delivery_id': 'ry96uihjrkzo0mr0ez32got', 'courier_id': '1vkfr8hpn52tm08zywtzve1', 'address': 'Ул. Заречная, 9, кв. 76', 'delivery_ts': '2022-12-29 03:28:29.465000', 'rate': 4, 'sum': 4957, 'tip_sum': 743}
{'order_id': '63ad03ec2fea41e65d1b5f6d', 'order_ts': '2022-12-29 03:05:16.668000', 'delivery_id': 'ies3v29qdzr8zn82cpo8krs', 'courier_id': 'm7xvsh36yz3hxspxcsyr7gy', 'address': 'Ул. Советская, 4, кв. 341', 'delivery_ts': '2022-12-29 04:16:18.637000', 'rate': 5, 'sum': 2536, 'tip_sum': 253}
{'order_id': '63ad0519e347a3b90ff41dd8', 'order_ts': '2022-12-29 03:10:17.521000', 'delivery_id': '6l4wz4fv0xyajbblu7ipsqb', 'courier_id': 't4fpxod2e6ctk2cgltpywnt', 'address': 'Ул. Металлургов, 14, кв. 375', 'delivery_ts': '2022-12-29 04:40:24.081000', 'rate': 5, 'sum': 1013, 'tip_sum': 50}
{'order_id': '63ad064511c6412eb447b8fa', 'order_ts': '2022-12-29 03:15:17.493000', 'delivery_id': 'lmqoa8pni5ba0grhvigck7q', 'courier_id': 'ubkl6e13p8sm1a9kaotnnzv', 'address': 'Ул. Новая, 13, кв. 184', 'delivery_ts': '2022-12-29 04:53:25.381000', 'rate': 5, 'sum': 2840, 'tip_sum': 284}
