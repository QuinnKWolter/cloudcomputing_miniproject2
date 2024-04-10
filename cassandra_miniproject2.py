# Group 17 - MiniProject 02: Quinn K Wolter, Abhigyan Kishore, Saleh Almutairi

from cassandra.cluster import Cluster
from collections import Counter

cluster = Cluster(['10.254.1.242', '10.254.3.9'])
session = cluster.connect('project2')

# Question 1 - How many hits were made to the website item “/administrator/index.php”?
print("[Q] Question 1 - How many hits were made to the website item “/administrator/index.php”?")
query = "SELECT COUNT(*) FROM web_logs WHERE path = '/administrator/index.php' ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"[A] Hits to /administrator/index.php: {row.count}")

# Question 2 - How many hits were made from the IP: 96.32.128.5
print("\n[Q] Question 2 - How many hits were made from the IP: 96.32.128.5")
query = "SELECT COUNT(*) FROM web_logs WHERE ip = '96.32.128.5' ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"[A] Hits from IP 96.32.128.5: {row.count}")

# Question 3 - Which path in the website has been hit most? How many hits were made to the path?
print("\n[Q] Question 3 - Which path in the website has been hit most? How many hits were made to the path?")
query = "SELECT path FROM web_logs;"
result = session.execute(query)
path_counts = Counter(row.path for row in result)
most_common_path, count = path_counts.most_common(1)[0]
print(f"[A] Most accessed path: {most_common_path} with {count} hits")

# Question 4 - Which IP accesses the website most? How many accesses were made by it?
print("\n[Q] Question 4 - Which IP accesses the website most? How many accesses were made by it?")
query = "SELECT ip FROM web_logs;"
result = session.execute(query)
ip_counts = Counter(row.ip for row in result)
most_common_ip, count = ip_counts.most_common(1)[0]
print(f"[A] IP with most accesses: {most_common_ip} with {count} accesses")

# Question 5 - How many accesses were made by Firefox(Mozilla)?
print("\n[Q] Question 5 - How many accesses were made by Firefox(Mozilla)?")
query = "SELECT COUNT(*) FROM web_logs WHERE user_agent LIKE '%Firefox%';"
result = session.execute(query)
for row in result:
    print(f"[A] Accesses by Firefox: {row.count}")

# Question 6 - For all requests on 02/Apr/2022, what is the ratio of GET request?
print("\n[Q] Question 6 - For all requests on 02/Apr/2022, what is the ratio of GET request?")
total_query = "SELECT COUNT(*) FROM web_logs WHERE access_date >= '2022-04-02' AND access_date < '2022-04-03' ALLOW FILTERING;"
total_result = session.execute(total_query)
get_query = "SELECT COUNT(*) FROM web_logs WHERE access_date >= '2022-04-02' AND access_date < '2022-04-03' AND method = 'GET' ALLOW FILTERING;"
get_result = session.execute(get_query)
total_count = total_result.one()[0] if total_result else 0
get_count = get_result.one()[0] if get_result else 0
ratio = get_count / total_count if total_count > 0 else 0
percentage = ratio * 100  # Convert ratio to percentage
print(f"[A] Ratio of GET requests on 02/Apr/2022: {ratio} ({percentage:.2f}%)")

# Question 7 - How many requests are lower than or equal to 404 bytes?
print("\n[Q] Question 7 - How many requests are lower than or equal to 404 bytes?")
query = "SELECT COUNT(*) FROM web_logs WHERE response_size <= 404 ALLOW FILTERING;"
result = session.execute(query)
for row in result:
    print(f"[A] Requests <= 404 bytes: {row.count}")

# Question 8 - List the IPs that have more than ten 404 requests. If no ip fulfills, print the ip that has most 404 requests and the number of requests.
print("\n[Q] Question 8 - List the IPs that have more than ten 404 requests. If no IP fulfills, print the IP that has most 404 requests and the number of requests.")
query = "SELECT ip FROM web_logs WHERE status_code = 404 ALLOW FILTERING;"
result = session.execute(query)
ip_404_counts = Counter(row.ip for row in result)
ips_with_more_than_10_404 = [(ip, count) for ip, count in ip_404_counts.items() if count > 10]
if ips_with_more_than_10_404:
    for ip, count in ips_with_more_than_10_404:
        print(f"[A] IP: {ip} has {count} 404 responses")
else:
    max_404_ip, max_404_count = ip_404_counts.most_common(1)[0]
    print(f"[A] IP with the most 404 responses: {max_404_ip} with {max_404_count} 404 responses")
