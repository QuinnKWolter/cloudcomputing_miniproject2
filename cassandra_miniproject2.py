from cassandra.cluster import Cluster
from collections import Counter

cluster = Cluster(['10.254.1.242', '10.254.3.9'])
session = cluster.connect('project2')

# Question 1
query = "SELECT COUNT(*) FROM web_logs WHERE path = '/administrator/index.php' ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"Hits to /administrator/index.php: {row.count}")

# Question 2
query = "SELECT COUNT(*) FROM web_logs WHERE ip = '96.32.128.5' ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"Hits from IP 96.32.128.5: {row.count}")

# Question 3
query = "SELECT path FROM web_logs;"
result = session.execute(query)
path_counts = Counter(row.path for row in result)
most_common_path, count = path_counts.most_common(1)[0]
print(f"Most accessed path: {most_common_path} with {count} hits")

# Question 4
query = "SELECT ip FROM web_logs;"
result = session.execute(query)
ip_counts = Counter(row.ip for row in result)
most_common_ip, count = ip_counts.most_common(1)[0]
print(f"IP with most accesses: {most_common_ip} with {count} accesses")

# Question 5
query = "SELECT COUNT(*) FROM web_logs WHERE user_agent LIKE '%Firefox%';"
result = session.execute(query)
for row in result:
    print(f"Accesses by Firefox: {row.count}")

# Question 6
total_query = "SELECT COUNT(*) FROM web_logs WHERE access_date >= '2022-04-02' AND access_date < '2022-04-03' ALLOW FILTERING;"
total_result = session.execute(total_query)
get_query = "SELECT COUNT(*) FROM web_logs WHERE access_date >= '2022-04-02' AND access_date < '2022-04-03' AND method = 'GET' ALLOW FILTERING;"
get_result = session.execute(get_query)
total_count = total_result.one()[0] if total_result else 0
get_count = get_result.one()[0] if get_result else 0
ratio = get_count / total_count if total_count > 0 else 0
percentage = ratio * 100
print(f"Ratio of GET requests on 02/Apr/2022: {ratio} ({percentage:.2f}%)")

# Question 7
query = "SELECT COUNT(*) FROM web_logs WHERE response_size <= 404 ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"Requests <= 404 bytes: {row.count}")

# Question 8
query = "SELECT ip FROM web_logs WHERE status_code = 404 ALLOW FILTERING;"
result = session.execute(query)
ip_404_counts = Counter(row.ip for row in result)
ips_with_more_than_10_404 = [(ip, count) for ip, count in ip_404_counts.items() if count > 10]
if ips_with_more_than_10_404:
    for ip, count in ips_with_more_than_10_404:
        print(f"IP: {ip} has {count} 404 responses")
else:
    max_404_ip, max_404_count = ip_404_counts.most_common(1)[0]
    print(f"IP with the most 404 responses: {max_404_ip} with {max_404_count} 404 responses")
