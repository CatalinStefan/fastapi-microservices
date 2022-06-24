import time
from main import redis, Order

key = 'refund-order'
group = 'payment'

try:
  redis.xgroup_create(name=key, groupname=group, mkstream=True)
  print("Group created")
except Exception as e:
  print(str(e))

while True:
  try:
    results = redis.xreadgroup(groupname=group, consumername=key, streams={key: '>'})
    print(results)
    if results != []:
      for result in results:
        obj = result[1][0][1]

        order = Order.get(obj['pk'])
        order.status = 'refunded'
        order.save()
        print(order)
  except Exception as e:
    print(str(e))
  time.sleep(3)