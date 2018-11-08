SELECT c_name FROM customer
WHERE c_custkey IN (SELECT o_custkey
					FROM orders
					WHERE o_totalprice >= 100000
					GROUP BY o_custkey
					HAVING COUNT(o_orderkey) >= 20
					ORDER BY COUNT(o_orderkey) DESC);