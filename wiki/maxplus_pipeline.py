"""
Max-Plus-Analyzer fuer wiederkehrende Daten-Pipelines (Demo).

Pipeline (dbt/Airflow-artig), Dauern in Stunden. Cross-Run-Kanten = Job in
Lauf k haengt an einem Job aus Lauf k-1 (inkrementeller State, Warm-Start,
Quality-Gate). Genau diese Kanten erzeugen Kreise ueber die Zeit, die den
Durchsatz begrenzen, egal wie viele Worker parallel laufen.

Fragen, die der Analyzer beantwortet:
  1. Minimale nachhaltige Zykluszeit lambda (Max-Plus-Eigenwert, Karp).
  2. Welcher Kreis ist schuld (kritischer Kreis)?
  3. Was passiert, wenn man schneller taktet als lambda (Drift-Simulation)?
  4. What-if-Ranking von Gegenmassnahmen.
"""

from itertools import permutations

NEG = float("-inf")

# ---------------- Pipeline-Definition ----------------
DUR = {"ingest": 0.7, "dq": 0.3, "core": 1.1, "features": 0.9,
       "retrain": 1.6, "score": 0.5, "monitor": 0.3, "reports": 0.4}
INTRA = [("ingest", "dq"), ("dq", "core"), ("core", "features"),
         ("features", "retrain"), ("retrain", "score"),
         ("score", "monitor"), ("score", "reports")]
CROSS = [("core", "core"),         # inkrementelles Modell braucht gestrigen State
         ("retrain", "retrain"),   # Warm-Start vom letzten Checkpoint
         ("retrain", "features"),  # Features nutzen Embeddings des letzten Modells
         ("monitor", "core")]      # Quality-Gate: gestriges Monitoring schaltet Core frei

JOBS = list(DUR)
IDX = {j: i for i, j in enumerate(JOBS)}

def toposort(jobs, intra):
    preds = {j: [u for u, v in intra if v == j] for j in jobs}
    order, seen = [], set()
    def visit(j):
        if j in seen: return
        for p in preds[j]: visit(p)
        seen.add(j); order.append(j)
    for j in jobs: visit(j)
    return order, preds

ORDER, PREDS = toposort(JOBS, INTRA)

def longest_intra_from(v, dur):
    """Laengster Pfad-Gesamtgewicht (inkl. Start- und Endjob) von v zu allen
    Nachfolgern im Intra-DAG, plus Pfad-Rekonstruktion."""
    dist = {j: NEG for j in JOBS}; dist[v] = dur[v]
    par = {j: None for j in JOBS}
    for j in ORDER:
        if dist[j] == NEG: continue
        for u, w in INTRA:
            if u == j and dist[j] + dur[w] > dist[w]:
                dist[w] = dist[j] + dur[w]; par[w] = j
    return dist, par

def build_Abar(dur, cross):
    """Kondensierte Matrix: Abar[ziel][quelle] = max Gewicht Quelle(k-1) -> Ziel(k)."""
    n = len(JOBS)
    A = [[NEG] * n for _ in range(n)]
    paths = {}
    for u, v in cross:                      # u aus Lauf k-1, v aus Lauf k
        dist, par = longest_intra_from(v, dur)
        for j in JOBS:
            if dist[j] > A[IDX[j]][IDX[u]]:
                A[IDX[j]][IDX[u]] = dist[j]
                p, node = [j], j
                while par[node] is not None: node = par[node]; p.append(node)
                paths[(u, j)] = list(reversed(p))
    return A, paths

def karp_lambda(A):
    """Karp: maximales Zyklusmittel der Matrix A (Kanten quelle->ziel)."""
    n = len(A)
    D = [[NEG] * n for _ in range(n + 1)]
    for v in range(n): D[0][v] = 0.0
    for k in range(1, n + 1):
        for v in range(n):
            best = NEG
            for u in range(n):
                if D[k - 1][u] > NEG and A[v][u] > NEG:
                    best = max(best, D[k - 1][u] + A[v][u])
            D[k][v] = best
    lam = NEG
    for v in range(n):
        if D[n][v] == NEG: continue
        m = min((D[n][v] - D[k][v]) / (n - k)
                for k in range(n) if D[k][v] > NEG)
        lam = max(lam, m)
    return lam

def critical_cycle(A, lam, tol=1e-9):
    """Kleinen Graphen direkt absuchen: Zyklus mit Mittel == lambda."""
    n = len(A)
    for L in range(1, n + 1):
        for cyc in permutations(range(n), L):
            w = 0.0; ok = True
            for i in range(L):
                a = A[cyc[(i + 1) % L]][cyc[i]]
                if a == NEG: ok = False; break
                w += a
            if ok and abs(w / L - lam) < tol:
                return list(cyc)
    return None

def simulate(dur, cross, T, K=10):
    """Vervollstaendigungszeiten c_j(k); Start jedes Laufs zum Release k*T."""
    prev, hist = None, []
    for k in range(K):
        c = {}
        for j in ORDER:
            t = k * T
            for p in PREDS[j]: t = max(t, c[p])
            if prev is not None:
                for u, v in cross:
                    if v == j: t = max(t, prev[u])
            c[j] = t + dur[j]
        hist.append(c["reports"] - k * T)   # Latenz des Laufs k
        prev = c
    return hist

def report(dur, cross, label):
    A, paths = build_Abar(dur, cross)
    lam = karp_lambda(A)
    print(f"{label}: lambda = {lam:.2f} h")
    return lam, A, paths

# ---------------- Analyse ----------------
print("=== Einzellauf-Sicht (was heutige Tools zeigen) ===")
dist0, _ = longest_intra_from("ingest", DUR)
cp = max(dist0.values())
print(f"Critical Path eines Laufs (Latenz): {cp:.1f} h")
print("-> Naive Schlussfolgerung: Laeufe ueberlappen, also ist jeder Takt moeglich,"
      "\n   nur die Latenz bleibt bei ~%.1f h.\n" % cp)

print("=== Max-Plus-Sicht ===")
lam, A, paths = report(DUR, CROSS, "Nachhaltige Zykluszeit")
cyc = critical_cycle(A, lam)
names = [JOBS[i] for i in cyc]
print(f"Kritischer Kreis (kondensiert): {' -> '.join(names)} -> {names[0]}")
for i in range(len(cyc)):
    u, j = JOBS[cyc[i]], JOBS[cyc[(i + 1) % len(cyc)]]
    if (u, j) in paths:
        print(f"  Segment {u}(k-1) -> {j}(k) via: {' -> '.join(paths[(u, j)])}")
print()

T = 3.0
print(f"=== Drift-Simulation bei Ziel-Takt T = {T:.1f} h ===")
hist = simulate(DUR, CROSS, T)
for k, h in enumerate(hist):
    print(f"  Lauf {k:2d}: Latenz {h:5.2f} h")
print(f"Drift/Lauf (letzte 5): {hist[-1] - hist[-6]:.2f} h ueber 5 Laeufe "
      f"= {(hist[-1] - hist[-6]) / 5:.2f} h/Lauf; Theorie lambda - T = {lam - T:.2f} h/Lauf\n")

print("=== What-if-Ranking ===")
d2 = dict(DUR); d2["retrain"] = 0.8
report(d2, CROSS, "(a) Retrain halbieren (0.8 h, GPU-Invest)")
c2 = [e for e in CROSS if e != ("monitor", "core")]
report(DUR, c2, "(b) Quality-Gate asynchron machen (Kante monitor->core weg)")
d3 = dict(DUR); d3["core"] = 0.55
report(d3, CROSS, "(c) Core-Job optimieren (1.1 -> 0.55 h)")
