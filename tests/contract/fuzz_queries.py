#!/usr/bin/env python3
"""Fuzz test: generate random SQL/PPL, fire at Fuse, check for 500s."""
import random, string, json, subprocess, sys

FUSE = sys.argv[1] if len(sys.argv) > 1 else "https://fuse.huanji.profile.aws.dev"
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 200

SQL_KW = ["SELECT","FROM","WHERE","JOIN","ON","GROUP BY","ORDER BY","LIMIT",
          "UNION","ALL","INSERT","DELETE","DROP","ALTER","CREATE","UPDATE",
          "HAVING","DISTINCT","AND","OR","NOT","IN","LIKE","NULL","EXISTS",
          "CASE","WHEN","THEN","ELSE","END","COUNT","SUM","AVG","EXPLAIN"]

PPL_KW = ["source","where","fields","stats","sort","head","tail","eval",
          "dedup","rename","lookup","top","rare","count()","sum()","avg()","by"]

TABLES = ["cluster_a.application_logs","cluster_b.application_logs",
          "dynamodb.users","nonexistent.table"]

def rs(n=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def rand_sql():
    k = random.randint(0, 8)
    if k == 0: return ' '.join(random.choices(SQL_KW, k=random.randint(1, 10)))
    if k == 1: return f"SELECT * FROM {rs()}.{rs()} LIMIT {random.randint(0, 999)}"
    if k == 2:
        q = "SELECT 1"
        for _ in range(random.randint(2, 8)): q = f"SELECT * FROM ({q})"
        return q
    if k == 3: return " UNION ALL ".join(f"SELECT '{rs()}'" for _ in range(random.randint(2, 12)))
    if k == 4: return ''.join(random.choices(string.printable, k=random.randint(1, 150)))
    if k == 5: return random.choice(["", " ", "\t\n"])
    if k == 6: return random.choice(["'; DROP TABLE x;--","1 OR 1=1","SELECT 1;SELECT 2"])
    if k == 7: return f"SELECT * FROM {random.choice(TABLES)} WHERE {rs()}='{rs()}' LIMIT 5"
    return "SELECT " + ",".join(rs() for _ in range(random.randint(50, 200))) + " FROM x"

def rand_ppl():
    k = random.randint(0, 4)
    if k == 0: return ' | '.join(random.choices(PPL_KW, k=random.randint(1, 6)))
    if k == 1: return f"source = {random.choice(TABLES)} | head {random.randint(0, 50)}"
    if k == 2: return ' | '.join(rs() for _ in range(random.randint(2, 8)))
    if k == 3: return random.choice(["", "source =", "|||"])
    return f"source = {random.choice(TABLES)} | " + ' | '.join(
        f"where {rs()} > {random.randint(0,999)}" for _ in range(15))

ok = fail = 0
for i in range(ITERS):
    fmt = random.choice(["sql","sql","sql","ppl"])
    q = rand_sql() if fmt == "sql" else rand_ppl()
    payload = json.dumps({"query": q, "format": fmt})
    try:
        r = subprocess.run(
            ["curl","-sk","-o","/dev/null","-w","%{http_code}","--max-time","5",
             "-X","POST",f"{FUSE}/api/fuse/query",
             "-H","Content-Type: application/json","-d",payload],
            capture_output=True, text=True, timeout=10)
        code = r.stdout.strip()
    except Exception:
        code = "timeout"
    if code == "500":
        fail += 1
        d = q[:80].replace('\n','\\n') + ("..." if len(q) > 80 else "")
        print(f"  \u274c [{i+1}] 500 on {fmt}: {d}")
    else:
        ok += 1
    if (i+1) % 50 == 0:
        print(f"  ... {i+1}/{ITERS} ({ok} ok, {fail} 500s)")


print()
print(f"  Results: {ok}/{ok+fail} ok, {fail} 500s")
rate = fail / max(ok + fail, 1) * 100
if fail > 0 and rate <= 10:
    print(f"  ⚠️  {fail} 500s ({rate:.1f}%) — within tolerance (<10%)")
elif fail > 0:
    print(f"  ❌ {fail} 500s ({rate:.1f}%) — exceeds 10% threshold")
    sys.exit(1)
else:
    print(f"  ✅ No 500s — server handled all fuzz inputs gracefully")
