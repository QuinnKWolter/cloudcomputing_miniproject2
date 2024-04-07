from cassandra.cluster import Cluster

cluster = Cluster(['10.254.2.62', '10.254.1.217', '10.254.0.206'])
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
query = "SELECT path, COUNT(*) FROM web_logs GROUP BY path ALLOW FILTERING;"
result = session.execute(query)
most_accessed_path = max(result, key=lambda x: x.count)
print(f"Most accessed path: {most_accessed_path.path} with {most_accessed_path.count} hits")

# Question 4
query = "SELECT ip, COUNT(*) FROM web_logs GROUP BY ip ALLOW FILTERING;"
result = session.execute(query)
most_accessing_ip = max(result, key=lambda x: x.count)
print(f"IP with most accesses: {most_accessing_ip.ip} with {most_accessing_ip.count} accesses")

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
print(f"Ratio of GET requests on 02/Apr/2022: {ratio}")

# Question 7
query = "SELECT COUNT(*) FROM web_logs WHERE response_size <= 404 ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"Requests <= 404 bytes: {row.count}")

# Question 8
query = "SELECT ip, COUNT(*) FROM web_logs WHERE status_code = 404 GROUP BY ip ALLOW FILTERING;"
result = session.execute(query)
ips_with_more_than_ten_404 = [row for row in result if row.count > 10]
if ips_with_more_than_ten_404:
    for ip_row in ips_with_more_than_ten_404:
        print(f"IP: {ip_row.ip} has {ip_row.count} 404 responses")
else:
    max_404_ip = max(result, key=lambda x: x.count)
    print(f"IP with the most 404 responses: {max_404_ip.ip} with {max_404_ip.count} 404 responses")
