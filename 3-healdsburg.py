# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pandas>=2.0",
#   "matplotlib>=3.7",
#   "numpy>=1.24",
#   "pyarrow>=14.0",
# ]
# ///
"""
Healdsburg Unified Math Performance Analysis
Generates 55+ charts into output-healdsburg/

Covers:
  - Overall performance trends vs California
  - Performance by grade (same grade over time)
  - Cohort tracking (same students through grades)
  - EL vs Fluent English breakdown
  - EL/Fluent by grade
  - EL mix and performance correlation
  - Heat maps (grade x year)
  - Additional subgroup analysis
"""

import warnings
from pathlib import Path

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── Output ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("output-healdsburg")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
PARQUET = "data/sbac_data.parquet"
MATH_TEST_ID = 2
GRADES = [3, 4, 5, 6, 7, 8, 11]
ALL_YEARS = [2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024, 2025]

# Student group names (exact strings from data)
SG_ALL      = "All Students"
SG_EL       = "EL (English learner, excluding RFEP)"
SG_FLUENT   = "IFEP, RFEP, and EO (Fluent English proficient and English only)"
SG_RFEP     = "RFEP (Reclassified fluent English proficient)"
SG_EO       = "EO (English only)"
SG_IFEP     = "IFEP (Initial fluent English proficient)"
SG_EVEREL   = "EverEL"
SG_NEVEREL  = "NeverEL"
SG_LTEL     = "LTEL (Long-Term English learner)"

# Colors
C_HB       = "#1f77b4"   # Healdsburg blue
C_CA       = "#ff7f0e"   # California orange
C_EL       = "#d62728"   # EL red
C_FLUENT   = "#2ca02c"   # Fluent green
C_HB_EL    = "#9467bd"   # Healdsburg EL purple
C_CA_EL    = "#e377c2"   # CA EL pink
C_HB_FL    = "#17becf"   # Healdsburg Fluent teal
C_CA_FL    = "#bcbd22"   # CA Fluent olive

# Global chart counter
_n = [0]

plt.rcParams.update({
    "figure.dpi": 130,
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 9,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def save(fig, slug):
    _n[0] += 1
    path = OUTPUT_DIR / f"{_n[0]:02d}_{slug}.png"
    fig.savefig(path, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"  [{_n[0]:02d}] {path.name}")


def pct_yaxis(ax):
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))


def set_ylim_pct(axes_list, pad=4, lo_min=0, hi_max=100):
    """Unified y-limits across a list of axes, with padding."""
    vals = []
    for ax in axes_list:
        for line in ax.get_lines():
            vals.extend([v for v in line.get_ydata() if np.isfinite(v)])
        for cont in ax.containers:
            for patch in cont:
                h = patch.get_height()
                if np.isfinite(h):
                    vals.append(h)
    if not vals:
        return
    lo = max(lo_min, min(vals) - pad)
    hi = min(hi_max, max(vals) + pad)
    for ax in axes_list:
        ax.set_ylim(lo, hi)


def wavg(df, group_cols):
    """Weighted average of pct_met_and_above by students_tested."""
    rows = []
    for keys, grp in df.groupby(group_cols, observed=True):
        g = grp.dropna(subset=["pct_met_and_above", "students_tested"])
        g = g[g["students_tested"] > 0]
        if g.empty:
            continue
        tot = g["students_tested"].sum()
        avg = (g["pct_met_and_above"] * g["students_tested"]).sum() / tot
        row = dict(zip(group_cols, keys if isinstance(keys, tuple) else [keys]))
        row["pct_met_and_above"] = avg
        row["students_tested"] = float(tot)
        rows.append(row)
    return pd.DataFrame(rows)


# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading parquet…")
raw = pd.read_parquet(PARQUET)
math = raw[raw["test_id"] == MATH_TEST_ID].copy()
print(f"  Math rows: {len(math):,}")

# Healdsburg district rows
HB = math[
    math["district_name"].str.contains("Healdsburg Unified", case=False, na=False)
    & (math["type_id"] == 6)
].copy()

# California state rows
CA = math[math["type_id"] == 4].copy()

print(f"  Healdsburg rows: {len(HB):,}")
print(f"  CA state rows:   {len(CA):,}")
print()


# ── Helper shortcuts ───────────────────────────────────────────────────────────
def hb(sg):
    return HB[HB["student_group_name"] == sg]

def ca(sg):
    return CA[CA["student_group_name"] == sg]

def hb_annual(sg):
    """Healdsburg: weighted avg across grades, by year."""
    return wavg(hb(sg), ["year"])

def ca_annual(sg):
    """CA: weighted avg across grades, by year."""
    return wavg(ca(sg), ["year"])

def hb_grade_annual(sg):
    """Healdsburg: by grade and year."""
    return hb(sg).dropna(subset=["pct_met_and_above"]).copy()

def ca_grade_annual(sg):
    """CA: by grade and year."""
    return ca(sg).dropna(subset=["pct_met_and_above"]).copy()

def series_xy(df, xcol, ycol):
    d = df[[xcol, ycol]].dropna().sort_values(xcol)
    return d[xcol].tolist(), d[ycol].tolist()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — OVERALL PERFORMANCE TRENDS (charts 1–5)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 1: Overall trends…")

# 1. Line: Healdsburg vs CA overall (weighted avg across grades)
hb_ov = hb_annual(SG_ALL)
ca_ov = ca_annual(SG_ALL)

fig, ax = plt.subplots(figsize=(9, 5))
hx, hy = series_xy(hb_ov, "year", "pct_met_and_above")
cx, cy = series_xy(ca_ov, "year", "pct_met_and_above")
ax.plot(hx, hy, "o-", color=C_HB, lw=2.5, label="Healdsburg Unified")
ax.plot(cx, cy, "s--", color=C_CA, lw=2.5, label="California")
for x, y in zip(hx, hy):
    ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, 7),
                ha="center", fontsize=7, color=C_HB)
for x, y in zip(cx, cy):
    ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, -13),
                ha="center", fontsize=7, color=C_CA)
ax.set_title("Math Performance Over Time: Healdsburg Unified vs California\n(All Students, All Grades — weighted avg)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
pct_yaxis(ax)
ax.set_xticks(ALL_YEARS)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "overall_line_hb_vs_ca")

# 2. Bar: Healdsburg vs CA by year (grouped)
fig, ax = plt.subplots(figsize=(11, 5))
hb_d = dict(zip(hb_ov["year"], hb_ov["pct_met_and_above"]))
ca_d = dict(zip(ca_ov["year"], ca_ov["pct_met_and_above"]))
x = np.arange(len(ALL_YEARS))
w = 0.38
bars_hb = ax.bar(x - w/2, [hb_d.get(y, np.nan) for y in ALL_YEARS], w,
                 color=C_HB, label="Healdsburg Unified", alpha=0.85)
bars_ca = ax.bar(x + w/2, [ca_d.get(y, np.nan) for y in ALL_YEARS], w,
                 color=C_CA, label="California", alpha=0.85)
ax.set_xticks(x)
ax.set_xticklabels(ALL_YEARS, rotation=45)
ax.set_title("Math Performance by Year: Healdsburg vs California\n(All Students, All Grades — grouped bars)")
ax.set_ylabel("% Meeting or Exceeding Standards")
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "overall_bars_hb_vs_ca")

# 3. Performance gap (HB − CA) over time
merged = hb_ov.set_index("year")[["pct_met_and_above"]].rename(
    columns={"pct_met_and_above": "hb"}).join(
    ca_ov.set_index("year")[["pct_met_and_above"]].rename(
        columns={"pct_met_and_above": "ca"}), how="inner")
merged["gap"] = merged["hb"] - merged["ca"]

fig, ax = plt.subplots(figsize=(9, 5))
years_g = merged.index.tolist()
gaps = merged["gap"].tolist()
colors = [C_HB if g >= 0 else "#e74c3c" for g in gaps]
ax.bar(years_g, gaps, color=colors, alpha=0.8)
ax.axhline(0, color="black", lw=1)
for yr, g in zip(years_g, gaps):
    ax.annotate(f"{g:+.1f}", (yr, g), textcoords="offset points",
                xytext=(0, 4 if g >= 0 else -12), ha="center", fontsize=8)
ax.set_title("Math Performance Gap: Healdsburg Unified minus California\n(Positive = Healdsburg above state avg, All Students)")
ax.set_xlabel("Year")
ax.set_ylabel("Percentage Point Difference")
ax.set_xticks(years_g)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "overall_gap_hb_minus_ca")

# 4. Year-over-year change in Healdsburg performance
hb_ov_s = hb_ov.set_index("year")["pct_met_and_above"].reindex(ALL_YEARS)
yoy = hb_ov_s.diff().dropna()

fig, ax = plt.subplots(figsize=(9, 5))
colors_yoy = [C_HB if v >= 0 else "#e74c3c" for v in yoy]
ax.bar(yoy.index, yoy.values, color=colors_yoy, alpha=0.85)
ax.axhline(0, color="black", lw=0.8)
for yr, v in zip(yoy.index, yoy.values):
    ax.annotate(f"{v:+.1f}", (yr, v), textcoords="offset points",
                xytext=(0, 4 if v >= 0 else -13), ha="center", fontsize=8)
ax.set_title("Year-over-Year Change in Math Performance — Healdsburg Unified\n(All Students, All Grades weighted avg; 2021 follows 2019)")
ax.set_xlabel("Year")
ax.set_ylabel("Change in % Meeting or Exceeding Standards (pp)")
ax.set_xticks(yoy.index)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "overall_yoy_change_hb")

# 5. Indexed performance (2015 = 100 baseline)
fig, ax = plt.subplots(figsize=(9, 5))
hb_idx = hb_ov.set_index("year")["pct_met_and_above"].reindex(ALL_YEARS)
ca_idx = ca_ov.set_index("year")["pct_met_and_above"].reindex(ALL_YEARS)
base_hb = hb_idx.get(2015, np.nan)
base_ca = ca_idx.get(2015, np.nan)
ax.plot(ALL_YEARS, (hb_idx / base_hb * 100).values, "o-", color=C_HB, lw=2.5,
        label=f"Healdsburg (2015 base = {base_hb:.1f}%)")
ax.plot(ALL_YEARS, (ca_idx / base_ca * 100).values, "s--", color=C_CA, lw=2.5,
        label=f"California (2015 base = {base_ca:.1f}%)")
ax.axhline(100, color="gray", lw=1, ls=":")
ax.set_title("Indexed Math Performance (2015 = 100): Healdsburg vs California\n(All Students — shows relative rate of change)")
ax.set_xlabel("Year")
ax.set_ylabel("Index (2015 = 100)")
ax.set_xticks(ALL_YEARS)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "overall_indexed_2015_baseline")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SAME GRADE OVER TIME (charts 6–13)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 2: By grade, same grade over time…")

hb_ga = hb_grade_annual(SG_ALL)
ca_ga = ca_grade_annual(SG_ALL)

# 6–12. One chart per grade
for grade in GRADES:
    hb_g = hb_ga[hb_ga["grade"] == grade].sort_values("year")
    ca_g = ca_ga[ca_ga["grade"] == grade].sort_values("year")
    if hb_g.empty:
        continue
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(hb_g["year"], hb_g["pct_met_and_above"], "o-", color=C_HB, lw=2.5,
            label="Healdsburg Unified")
    ax.plot(ca_g["year"], ca_g["pct_met_and_above"], "s--", color=C_CA, lw=2.5,
            label="California")
    for _, row in hb_g.iterrows():
        if np.isfinite(row["pct_met_and_above"]):
            ax.annotate(f"{row['pct_met_and_above']:.0f}%",
                        (row["year"], row["pct_met_and_above"]),
                        textcoords="offset points", xytext=(0, 7),
                        ha="center", fontsize=7, color=C_HB)
    ax.set_title(f"Grade {grade} Math Performance Over Time: Healdsburg vs California\n(All Students — same grade tracked year to year)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, f"grade{grade:02d}_line_hb_vs_ca")

# 13. Facet grid: all grades, Healdsburg vs CA
fig, axes = plt.subplots(2, 4, figsize=(18, 9), sharey=False)
fig.suptitle("Math Performance by Grade Over Time: Healdsburg vs California\n(All Students — each panel = one grade)", fontsize=13, y=1.01)
axes_flat = axes.flatten()
all_grade_vals = []
for grade in GRADES:
    hb_g = hb_ga[hb_ga["grade"] == grade].sort_values("year")
    ca_g = ca_ga[ca_ga["grade"] == grade].sort_values("year")
    all_grade_vals.extend(hb_g["pct_met_and_above"].dropna().tolist())
    all_grade_vals.extend(ca_g["pct_met_and_above"].dropna().tolist())

ylim_lo = max(0, min(all_grade_vals) - 5)
ylim_hi = min(100, max(all_grade_vals) + 5)

for i, grade in enumerate(GRADES):
    ax = axes_flat[i]
    hb_g = hb_ga[hb_ga["grade"] == grade].sort_values("year")
    ca_g = ca_ga[ca_ga["grade"] == grade].sort_values("year")
    ax.plot(hb_g["year"], hb_g["pct_met_and_above"], "o-", color=C_HB, lw=2,
            label="Healdsburg")
    ax.plot(ca_g["year"], ca_g["pct_met_and_above"], "s--", color=C_CA, lw=2,
            label="California")
    ax.set_title(f"Grade {grade}", fontsize=11)
    ax.set_xticks(ALL_YEARS)
    ax.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
    ax.set_ylim(ylim_lo, ylim_hi)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("% Met or Exceeded")
    ax.set_xlabel("Year")
    ax.legend(fontsize=7)
    ax.grid(axis="y", alpha=0.3)

axes_flat[-1].set_visible(False)
fig.tight_layout()
save(fig, "facet_all_grades_hb_vs_ca")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — COHORT ANALYSIS, ALL STUDENTS (charts 14–18)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 3: Cohort analysis…")

hb_cohort_all = hb(SG_ALL).dropna(subset=["pct_met_and_above", "cohort_year"]).copy()
ca_cohort_all = ca(SG_ALL).dropna(subset=["pct_met_and_above", "cohort_year"]).copy()
cohort_years = sorted(hb_cohort_all["cohort_year"].unique())

COHORT_COLORS = plt.cm.tab10(np.linspace(0, 1, max(len(cohort_years), 1)))

# 14. Cohort spaghetti — Healdsburg only
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(cohort_years):
    d = hb_cohort_all[hb_cohort_all["cohort_year"] == cy].sort_values("grade")
    if len(d) < 2:
        continue
    ax.plot(d["grade"], d["pct_met_and_above"], "o-", color=COHORT_COLORS[i % len(COHORT_COLORS)],
            lw=1.8, alpha=0.85, label=f"Cohort {int(cy)}")
ax.set_title("Cohort Progression Through Grades — Healdsburg Unified Math\n(Each line = students who were 3rd graders in a given year)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_spaghetti_hb")

# 15. Cohort spaghetti — Healdsburg vs CA overlay (2 lines per cohort)
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(cohort_years):
    col = COHORT_COLORS[i % len(COHORT_COLORS)]
    d_hb = hb_cohort_all[hb_cohort_all["cohort_year"] == cy].sort_values("grade")
    d_ca = ca_cohort_all[ca_cohort_all["cohort_year"] == cy].sort_values("grade")
    if len(d_hb) < 2:
        continue
    ax.plot(d_hb["grade"], d_hb["pct_met_and_above"], "o-", color=col, lw=2, alpha=0.9,
            label=f"HB {int(cy)}")
    ax.plot(d_ca["grade"], d_ca["pct_met_and_above"], "x--", color=col, lw=1.2, alpha=0.5)
ax.set_title("Cohort Progression: Healdsburg (solid) vs California (dashed)\n(Same color = same cohort; line per cohort)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=7, ncol=2)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_spaghetti_hb_vs_ca")

# 16. Cohort bubble chart — size = students tested at grade 5
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(cohort_years):
    d = hb_cohort_all[hb_cohort_all["cohort_year"] == cy].sort_values("grade")
    if len(d) < 2:
        continue
    sizes = (d["students_tested"].fillna(20) * 3).clip(20, 800)
    ax.scatter(d["grade"], d["pct_met_and_above"], s=sizes,
               color=COHORT_COLORS[i % len(COHORT_COLORS)], alpha=0.6,
               label=f"Cohort {int(cy)}", edgecolors="white", lw=0.5)
    ax.plot(d["grade"], d["pct_met_and_above"], "-",
            color=COHORT_COLORS[i % len(COHORT_COLORS)], lw=1, alpha=0.4)
ax.set_title("Cohort Progression — Healdsburg Math (bubble size = students tested)\n(Each color = one cohort year)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_bubble_hb")

# 17. Recent cohorts highlighted (2016–2021 cohort years)
recent_cohorts = [cy for cy in cohort_years if cy >= 2016]
fig, ax = plt.subplots(figsize=(10, 6))
# Faint background: older cohorts
for cy in cohort_years:
    if cy in recent_cohorts:
        continue
    d = hb_cohort_all[hb_cohort_all["cohort_year"] == cy].sort_values("grade")
    ax.plot(d["grade"], d["pct_met_and_above"], "o-", color="lightgray", lw=1.2, alpha=0.6)
# Highlighted: recent cohorts
cmap = plt.cm.viridis(np.linspace(0.1, 0.9, len(recent_cohorts)))
for i, cy in enumerate(sorted(recent_cohorts)):
    d = hb_cohort_all[hb_cohort_all["cohort_year"] == cy].sort_values("grade")
    if len(d) < 2:
        continue
    ax.plot(d["grade"], d["pct_met_and_above"], "o-", color=cmap[i], lw=2.5,
            label=f"Cohort {int(cy)}", zorder=5)
ax.set_title("Recent Cohorts Highlighted — Healdsburg Math Performance\n(Older cohorts in gray; colored = cohort's 3rd-grade entry year)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(loc="upper right", fontsize=9)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_recent_highlighted")

# 18. Cohort start vs end: grade 3 vs grade 8 performance
starts, ends, cys_valid = [], [], []
for cy in cohort_years:
    d = hb_cohort_all[hb_cohort_all["cohort_year"] == cy]
    g3 = d[d["grade"] == 3]["pct_met_and_above"].values
    g8 = d[d["grade"] == 8]["pct_met_and_above"].values
    if len(g3) and len(g8) and np.isfinite(g3[0]) and np.isfinite(g8[0]):
        starts.append(g3[0])
        ends.append(g8[0])
        cys_valid.append(int(cy))

fig, ax = plt.subplots(figsize=(8, 5))
colors_cohort = plt.cm.tab10(np.linspace(0, 1, len(cys_valid)))
for i, (cy, s, e) in enumerate(zip(cys_valid, starts, ends)):
    ax.annotate("", xy=(e, i), xytext=(s, i),
                arrowprops=dict(arrowstyle="->", color=colors_cohort[i], lw=2))
    ax.plot([s, e], [i, i], color=colors_cohort[i], lw=2, alpha=0.6)
    ax.scatter([s], [i], color=colors_cohort[i], s=80, zorder=5)
    ax.scatter([e], [i], color=colors_cohort[i], s=80, marker="D", zorder=5)
    ax.annotate(f"Gr3={s:.0f}%  →  Gr8={e:.0f}%", (max(s, e) + 0.5, i),
                va="center", fontsize=8)
ax.set_yticks(range(len(cys_valid)))
ax.set_yticklabels([f"Cohort {c}" for c in cys_valid])
ax.set_xlabel("% Meeting or Exceeding Standards")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax.set_title("Grade 3 → Grade 8 Performance Change by Cohort — Healdsburg Math\n(Circle = Grade 3 start; Diamond = Grade 8 end)")
ax.grid(axis="x", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_start_vs_end_gr3_gr8")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — EL vs FLUENT OVERALL (charts 19–24)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 4: EL vs Fluent overall…")

hb_el_ann = hb_annual(SG_EL)
ca_el_ann = ca_annual(SG_EL)
hb_fl_ann = hb_annual(SG_FLUENT)
ca_fl_ann = ca_annual(SG_FLUENT)
hb_all_ann = hb_annual(SG_ALL)
ca_all_ann = ca_annual(SG_ALL)

# 19. EL performance: Healdsburg vs CA
fig, ax = plt.subplots(figsize=(9, 5))
hx, hy = series_xy(hb_el_ann, "year", "pct_met_and_above")
cx, cy_ = series_xy(ca_el_ann, "year", "pct_met_and_above")
ax.plot(hx, hy, "o-", color=C_HB_EL, lw=2.5, label="Healdsburg EL students")
ax.plot(cx, cy_, "s--", color=C_CA_EL, lw=2.5, label="California EL students")
for x, y in zip(hx, hy):
    ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, 7),
                ha="center", fontsize=7, color=C_HB_EL)
ax.set_title("EL Math Performance Over Time: Healdsburg vs California\n(English Learners only, excluding RFEP — all grades weighted avg)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(ALL_YEARS)
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_overall_line_hb_vs_ca")

# 20. Fluent performance: Healdsburg vs CA
fig, ax = plt.subplots(figsize=(9, 5))
hx, hy = series_xy(hb_fl_ann, "year", "pct_met_and_above")
cx, cy_ = series_xy(ca_fl_ann, "year", "pct_met_and_above")
ax.plot(hx, hy, "o-", color=C_HB_FL, lw=2.5, label="Healdsburg Fluent (IFEP+RFEP+EO)")
ax.plot(cx, cy_, "s--", color=C_CA_FL, lw=2.5, label="California Fluent (IFEP+RFEP+EO)")
for x, y in zip(hx, hy):
    ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, 7),
                ha="center", fontsize=7, color=C_HB_FL)
ax.set_title("Fluent-English Math Performance Over Time: Healdsburg vs California\n(IFEP + RFEP + EO combined — all grades weighted avg)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(ALL_YEARS)
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "fluent_overall_line_hb_vs_ca")

# 21. Side-by-side bars: EL vs Fluent by year (Healdsburg)
fig, ax = plt.subplots(figsize=(11, 5))
el_d = dict(zip(hb_el_ann["year"], hb_el_ann["pct_met_and_above"]))
fl_d = dict(zip(hb_fl_ann["year"], hb_fl_ann["pct_met_and_above"]))
x = np.arange(len(ALL_YEARS))
w = 0.38
ax.bar(x - w/2, [el_d.get(y, np.nan) for y in ALL_YEARS], w,
       color=C_EL, label="EL (English Learner)", alpha=0.85)
ax.bar(x + w/2, [fl_d.get(y, np.nan) for y in ALL_YEARS], w,
       color=C_FLUENT, label="Fluent (IFEP+RFEP+EO)", alpha=0.85)
ax.set_xticks(x)
ax.set_xticklabels(ALL_YEARS, rotation=45)
ax.set_title("EL vs Fluent-English Math Performance by Year — Healdsburg Unified\n(All grades weighted avg; side-by-side bars)")
ax.set_ylabel("% Meeting or Exceeding Standards")
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_vs_fluent_bars_hb")

# 22. EL vs Fluent gap (Healdsburg), reference to CA gap
def compute_gap(df_a, df_b):
    a = df_a.set_index("year")["pct_met_and_above"]
    b = df_b.set_index("year")["pct_met_and_above"]
    return (b - a).dropna()  # Fluent - EL (positive = fluent outperforms)

hb_gap = compute_gap(hb_el_ann, hb_fl_ann)
ca_gap = compute_gap(ca_el_ann, ca_fl_ann)

fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(hb_gap.index, hb_gap.values, "o-", color=C_HB, lw=2.5,
        label="Healdsburg: Fluent − EL gap")
ax.plot(ca_gap.index, ca_gap.values, "s--", color=C_CA, lw=2.5,
        label="California: Fluent − EL gap")
ax.set_title("EL vs Fluent-English Performance Gap Over Time\n(Fluent minus EL; larger = wider gap between groups)")
ax.set_xlabel("Year")
ax.set_ylabel("Performance Gap (percentage points)")
ax.set_xticks(ALL_YEARS)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_vs_fluent_gap_over_time")

# 23. EL% share over time: Healdsburg vs CA
def compute_el_share(district_df, state_df):
    el = district_df[district_df["student_group_name"] == SG_EL]
    all_s = district_df[district_df["student_group_name"] == SG_ALL]
    el_yr = el.groupby("year")["students_tested"].sum().rename("el")
    all_yr = all_s.groupby("year")["students_tested"].sum().rename("all")
    merged = pd.concat([el_yr, all_yr], axis=1).dropna()
    merged["el_pct"] = merged["el"] / merged["all"] * 100
    return merged["el_pct"]

hb_el_pct = compute_el_share(HB, CA)
ca_el = CA[CA["student_group_name"] == SG_EL]
ca_all_s = CA[CA["student_group_name"] == SG_ALL]
ca_el_yr = ca_el.groupby("year")["students_tested"].sum().rename("el")
ca_all_yr = ca_all_s.groupby("year")["students_tested"].sum().rename("all")
ca_el_merged = pd.concat([ca_el_yr, ca_all_yr], axis=1).dropna()
ca_el_pct = (ca_el_merged["el"] / ca_el_merged["all"] * 100)

fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(hb_el_pct.index, hb_el_pct.values, "o-", color=C_HB, lw=2.5,
        label="Healdsburg EL Share")
ax.plot(ca_el_pct.index, ca_el_pct.values, "s--", color=C_CA, lw=2.5,
        label="California EL Share")
for yr, v in zip(hb_el_pct.index, hb_el_pct.values):
    ax.annotate(f"{v:.1f}%", (yr, v), textcoords="offset points", xytext=(0, 7),
                ha="center", fontsize=7, color=C_HB)
ax.set_title("EL Student Share Over Time: Healdsburg vs California\n(EL students as % of all students tested in math)")
ax.set_xlabel("Year")
ax.set_ylabel("% of Students who are English Learners")
pct_yaxis(ax)
ax.set_xticks(sorted(hb_el_pct.index.tolist()))
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_share_over_time_hb_vs_ca")

# 24. EL and Fluent student counts over time (Healdsburg)
el_counts = HB[HB["student_group_name"] == SG_EL].groupby("year")["students_tested"].sum()
fl_counts = HB[HB["student_group_name"] == SG_FLUENT].groupby("year")["students_tested"].sum()
all_counts = HB[HB["student_group_name"] == SG_ALL].groupby("year")["students_tested"].sum()

fig, ax = plt.subplots(figsize=(10, 5))
yrs = sorted(set(el_counts.index) | set(fl_counts.index))
ax.bar(yrs, [el_counts.get(y, 0) for y in yrs], label="EL", color=C_EL, alpha=0.85)
ax.bar(yrs, [fl_counts.get(y, 0) for y in yrs],
       bottom=[el_counts.get(y, 0) for y in yrs],
       label="Fluent (IFEP+RFEP+EO)", color=C_FLUENT, alpha=0.85)
ax.plot(sorted(all_counts.index), all_counts.values, "ko--", lw=2, ms=7,
        label="All Students total", zorder=5)
ax.set_title("EL vs Fluent-English Student Counts by Year — Healdsburg Unified\n(Stacked bars = EL + Fluent; line = all students tested)")
ax.set_xlabel("Year")
ax.set_ylabel("Students Tested")
ax.legend()
ax.set_xticks(yrs)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_vs_fluent_counts_hb")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — EL / FLUENT BY GRADE (charts 25–34)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 5: EL/Fluent by grade…")

hb_el_ga = hb_grade_annual(SG_EL)
ca_el_ga = ca_grade_annual(SG_EL)
hb_fl_ga = hb_grade_annual(SG_FLUENT)
ca_fl_ga = ca_grade_annual(SG_FLUENT)

# 25. Facet: EL vs Fluent by grade over time (Healdsburg only)
fig, axes = plt.subplots(2, 4, figsize=(18, 9), sharey=False)
fig.suptitle("EL vs Fluent-English Math Performance by Grade — Healdsburg Unified\n(Red = EL; Green = Fluent English; each panel = one grade)", fontsize=13, y=1.01)
ax_flat = axes.flatten()
all_vals_25 = (hb_el_ga["pct_met_and_above"].dropna().tolist() +
               hb_fl_ga["pct_met_and_above"].dropna().tolist())
yl25 = (max(0, min(all_vals_25) - 5), min(100, max(all_vals_25) + 5))

for i, grade in enumerate(GRADES):
    ax = ax_flat[i]
    el = hb_el_ga[hb_el_ga["grade"] == grade].sort_values("year")
    fl = hb_fl_ga[hb_fl_ga["grade"] == grade].sort_values("year")
    ax.plot(el["year"], el["pct_met_and_above"], "o-", color=C_EL, lw=2, label="EL")
    ax.plot(fl["year"], fl["pct_met_and_above"], "s-", color=C_FLUENT, lw=2, label="Fluent")
    ax.set_title(f"Grade {grade}", fontsize=11)
    ax.set_xticks(ALL_YEARS)
    ax.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
    ax.set_ylim(*yl25)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("% Met or Exceeded")
    ax.set_xlabel("Year")
    ax.legend(fontsize=7)
    ax.grid(axis="y", alpha=0.3)

ax_flat[-1].set_visible(False)
fig.tight_layout()
save(fig, "facet_el_vs_fluent_by_grade_hb")

# 26. Facet: EL performance — Healdsburg vs CA by grade
fig, axes = plt.subplots(2, 4, figsize=(18, 9), sharey=False)
fig.suptitle("EL Math Performance by Grade: Healdsburg vs California\n(English Learners — each panel = one grade)", fontsize=13, y=1.01)
ax_flat = axes.flatten()
all_vals_26 = (hb_el_ga["pct_met_and_above"].dropna().tolist() +
               ca_el_ga["pct_met_and_above"].dropna().tolist())
yl26 = (max(0, min(all_vals_26) - 5), min(100, max(all_vals_26) + 5))

for i, grade in enumerate(GRADES):
    ax = ax_flat[i]
    hb_g = hb_el_ga[hb_el_ga["grade"] == grade].sort_values("year")
    ca_g = ca_el_ga[ca_el_ga["grade"] == grade].sort_values("year")
    ax.plot(hb_g["year"], hb_g["pct_met_and_above"], "o-", color=C_HB_EL, lw=2, label="Healdsburg EL")
    ax.plot(ca_g["year"], ca_g["pct_met_and_above"], "s--", color=C_CA_EL, lw=2, label="CA EL")
    ax.set_title(f"Grade {grade}", fontsize=11)
    ax.set_xticks(ALL_YEARS)
    ax.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
    ax.set_ylim(*yl26)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("% Met or Exceeded")
    ax.set_xlabel("Year")
    ax.legend(fontsize=7)
    ax.grid(axis="y", alpha=0.3)

ax_flat[-1].set_visible(False)
fig.tight_layout()
save(fig, "facet_el_by_grade_hb_vs_ca")

# 27. Facet: Fluent performance — Healdsburg vs CA by grade
fig, axes = plt.subplots(2, 4, figsize=(18, 9), sharey=False)
fig.suptitle("Fluent-English Math Performance by Grade: Healdsburg vs California\n(IFEP+RFEP+EO — each panel = one grade)", fontsize=13, y=1.01)
ax_flat = axes.flatten()
all_vals_27 = (hb_fl_ga["pct_met_and_above"].dropna().tolist() +
               ca_fl_ga["pct_met_and_above"].dropna().tolist())
yl27 = (max(0, min(all_vals_27) - 5), min(100, max(all_vals_27) + 5))

for i, grade in enumerate(GRADES):
    ax = ax_flat[i]
    hb_g = hb_fl_ga[hb_fl_ga["grade"] == grade].sort_values("year")
    ca_g = ca_fl_ga[ca_fl_ga["grade"] == grade].sort_values("year")
    ax.plot(hb_g["year"], hb_g["pct_met_and_above"], "o-", color=C_HB_FL, lw=2, label="Healdsburg Fluent")
    ax.plot(ca_g["year"], ca_g["pct_met_and_above"], "s--", color=C_CA_FL, lw=2, label="CA Fluent")
    ax.set_title(f"Grade {grade}", fontsize=11)
    ax.set_xticks(ALL_YEARS)
    ax.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
    ax.set_ylim(*yl27)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("% Met or Exceeded")
    ax.set_xlabel("Year")
    ax.legend(fontsize=7)
    ax.grid(axis="y", alpha=0.3)

ax_flat[-1].set_visible(False)
fig.tight_layout()
save(fig, "facet_fluent_by_grade_hb_vs_ca")

# 28–34. Individual grade: EL vs Fluent, Healdsburg vs CA (4 lines)
all_vals_ind = (hb_el_ga["pct_met_and_above"].dropna().tolist() +
                ca_el_ga["pct_met_and_above"].dropna().tolist() +
                hb_fl_ga["pct_met_and_above"].dropna().tolist() +
                ca_fl_ga["pct_met_and_above"].dropna().tolist())
yl_ind = (max(0, min(all_vals_ind) - 5), min(100, max(all_vals_ind) + 5))

for grade in GRADES:
    fig, ax = plt.subplots(figsize=(9, 5))
    hb_el_g = hb_el_ga[hb_el_ga["grade"] == grade].sort_values("year")
    ca_el_g = ca_el_ga[ca_el_ga["grade"] == grade].sort_values("year")
    hb_fl_g = hb_fl_ga[hb_fl_ga["grade"] == grade].sort_values("year")
    ca_fl_g = ca_fl_ga[ca_fl_ga["grade"] == grade].sort_values("year")
    ax.plot(hb_el_g["year"], hb_el_g["pct_met_and_above"], "o-", color=C_HB_EL, lw=2.5,
            label="Healdsburg EL")
    ax.plot(ca_el_g["year"], ca_el_g["pct_met_and_above"], "o--", color=C_CA_EL, lw=2,
            label="California EL")
    ax.plot(hb_fl_g["year"], hb_fl_g["pct_met_and_above"], "s-", color=C_HB_FL, lw=2.5,
            label="Healdsburg Fluent")
    ax.plot(ca_fl_g["year"], ca_fl_g["pct_met_and_above"], "s--", color=C_CA_FL, lw=2,
            label="California Fluent")
    ax.set_title(f"Grade {grade} Math: EL vs Fluent-English, Healdsburg vs California\n(4 lines: Healdsburg EL, CA EL, Healdsburg Fluent, CA Fluent)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.set_ylim(*yl_ind)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, f"grade{grade:02d}_el_vs_fluent_hb_vs_ca")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — ELL MIX AND PERFORMANCE CORRELATION (charts 35–40)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 6: ELL mix and correlation…")

# Build a per-year dataframe with EL%, EL perf, Fluent perf, overall perf
def build_mix_df():
    rows = []
    for yr in ALL_YEARS:
        el_row = hb_el_ann[hb_el_ann["year"] == yr]
        fl_row = hb_fl_ann[hb_fl_ann["year"] == yr]
        all_row = hb_all_ann[hb_all_ann["year"] == yr]
        el_p = el_row["pct_met_and_above"].values[0] if len(el_row) else np.nan
        fl_p = fl_row["pct_met_and_above"].values[0] if len(fl_row) else np.nan
        ov_p = all_row["pct_met_and_above"].values[0] if len(all_row) else np.nan
        el_n = hb_el_ann[hb_el_ann["year"] == yr]["students_tested"].values
        all_n = hb_all_ann[hb_all_ann["year"] == yr]["students_tested"].values
        el_pct = (el_n[0] / all_n[0] * 100) if len(el_n) and len(all_n) and all_n[0] > 0 else np.nan
        rows.append({"year": yr, "el_pct": el_pct, "el_perf": el_p,
                     "fluent_perf": fl_p, "overall_perf": ov_p})
    return pd.DataFrame(rows).dropna(subset=["el_pct", "overall_perf"])

mix = build_mix_df()

# 35. Scatter: EL% vs overall performance
fig, ax = plt.subplots(figsize=(7, 6))
sc = ax.scatter(mix["el_pct"], mix["overall_perf"],
                c=mix["year"], cmap="viridis", s=120, zorder=5)
for _, row in mix.iterrows():
    ax.annotate(str(int(row["year"])), (row["el_pct"], row["overall_perf"]),
                textcoords="offset points", xytext=(6, 3), fontsize=8)
if len(mix) >= 3:
    z = np.polyfit(mix["el_pct"].dropna(), mix["overall_perf"].dropna(), 1)
    p = np.poly1d(z)
    xs = np.linspace(mix["el_pct"].min(), mix["el_pct"].max(), 50)
    ax.plot(xs, p(xs), "r--", lw=1.5, alpha=0.6, label=f"Trend (slope={z[0]:.1f})")
    ax.legend(fontsize=9)
plt.colorbar(sc, ax=ax, label="Year")
ax.set_xlabel("EL Students as % of All Students Tested")
ax.set_ylabel("Overall Math % Meeting or Exceeding Standards")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
pct_yaxis(ax)
ax.set_title("EL Share vs Overall Math Performance — Healdsburg\n(Each dot = one year; negative slope suggests higher EL% → lower avg)")
ax.grid(alpha=0.3)
fig.tight_layout()
save(fig, "scatter_el_pct_vs_overall_perf")

# 36. Scatter: year-over-year Δ EL% vs Δ overall performance
mix_s = mix.set_index("year")
delta_el = mix_s["el_pct"].diff().dropna()
delta_ov = mix_s["overall_perf"].diff().dropna()
common_idx = delta_el.index.intersection(delta_ov.index)
delta_el = delta_el.loc[common_idx]
delta_ov = delta_ov.loc[common_idx]

fig, ax = plt.subplots(figsize=(7, 6))
ax.scatter(delta_el, delta_ov, c=common_idx, cmap="viridis", s=120, zorder=5)
for yr, de, do in zip(common_idx, delta_el, delta_ov):
    ax.annotate(str(int(yr)), (de, do), textcoords="offset points", xytext=(6, 3), fontsize=8)
ax.axhline(0, color="gray", lw=0.8, ls="--")
ax.axvline(0, color="gray", lw=0.8, ls="--")
ax.set_xlabel("Year-over-Year Change in EL% (pp)")
ax.set_ylabel("Year-over-Year Change in Overall Performance (pp)")
ax.set_title("Does Change in EL Share Predict Change in Performance? — Healdsburg\n(Each dot = one year; top-left quadrant: fewer EL, better perf)")
ax.grid(alpha=0.3)
fig.tight_layout()
save(fig, "scatter_delta_el_pct_vs_delta_perf")

# 37. Dual axis: EL% and overall performance over time
fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()
ax1.bar(mix["year"], mix["el_pct"], color=C_EL, alpha=0.3, label="EL % (left axis)")
ax1.plot(mix["year"], mix["el_pct"], "o-", color=C_EL, lw=2)
ax2.plot(mix["year"], mix["overall_perf"], "s-", color=C_HB, lw=2.5,
         label="Overall Performance (right axis)")
ax1.set_xlabel("Year")
ax1.set_ylabel("EL Students (% of all tested)", color=C_EL)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax1.tick_params(axis="y", labelcolor=C_EL)
ax2.set_ylabel("% Meeting or Exceeding Standards", color=C_HB)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax2.tick_params(axis="y", labelcolor=C_HB)
ax1.set_xticks(mix["year"])
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
ax1.set_title("EL Share and Overall Math Performance Over Time — Healdsburg Unified\n(Bars + left = EL%; line + right = overall % meeting standards)")
fig.tight_layout()
save(fig, "dual_axis_el_pct_and_overall_perf")

# 38. Simulated performance if EL% stayed constant at 2015 level
base_yr = mix[mix["year"] == 2015]
if not base_yr.empty:
    base_el_pct = base_yr["el_pct"].values[0] / 100
    mix["simulated_perf"] = (
        base_el_pct * mix["el_perf"] +
        (1 - base_el_pct) * mix["fluent_perf"]
    )
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(mix["year"], mix["overall_perf"], "o-", color=C_HB, lw=2.5,
            label="Actual Overall Performance")
    ax.plot(mix["year"], mix["simulated_perf"], "^--", color="#e67e22", lw=2.5,
            label=f"Simulated: if EL% stayed at {base_el_pct*100:.1f}% (2015 level)")
    ax.fill_between(mix["year"], mix["overall_perf"], mix["simulated_perf"],
                    alpha=0.15, color="#e67e22",
                    label="Difference = effect of EL mix change")
    ax.set_title("Counterfactual: What if EL Share Had Stayed at 2015 Level? — Healdsburg Math\n(Simulated = actual EL/Fluent performance, but with 2015 EL mix)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    pct_yaxis(ax)
    ax.set_xticks(mix["year"].tolist())
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, "counterfactual_constant_el_pct")

# 39. Performance decomposition: EL contribution + Fluent contribution
mix_clean = mix.dropna(subset=["el_pct", "el_perf", "fluent_perf"]).copy()
mix_clean["el_contrib"]     = mix_clean["el_pct"] / 100 * mix_clean["el_perf"]
mix_clean["fluent_contrib"] = (1 - mix_clean["el_pct"] / 100) * mix_clean["fluent_perf"]

fig, ax = plt.subplots(figsize=(11, 5))
ax.bar(mix_clean["year"], mix_clean["el_contrib"],
       label="EL contribution (EL% × EL perf)", color=C_EL, alpha=0.85)
ax.bar(mix_clean["year"], mix_clean["fluent_contrib"],
       bottom=mix_clean["el_contrib"],
       label="Fluent contribution (Fluent% × Fluent perf)", color=C_FLUENT, alpha=0.85)
ax.plot(mix_clean["year"], mix_clean["overall_perf"], "ko--", lw=2, ms=7,
        label="Actual overall performance", zorder=5)
ax.set_title("Performance Decomposition: EL + Fluent Contribution to Overall Math Score\n(Stacked = weighted contribution of each group to overall %; black = actual)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
pct_yaxis(ax)
ax.set_xticks(mix_clean["year"].tolist())
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "performance_decomposition_el_fluent")

# 40. EL perf, Fluent perf, and overall perf — 3 lines on one chart
fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(mix_clean["year"], mix_clean["el_perf"], "o-", color=C_EL, lw=2.5,
        label="EL Performance")
ax.plot(mix_clean["year"], mix_clean["fluent_perf"], "s-", color=C_FLUENT, lw=2.5,
        label="Fluent Performance")
ax.plot(mix_clean["year"], mix_clean["overall_perf"], "^-", color=C_HB, lw=2.5,
        label="Overall Performance (all students)")
ax.set_title("EL, Fluent, and Overall Math Performance — Healdsburg Unified\n(All grades weighted avg; shows overall as mix of two groups)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
pct_yaxis(ax)
ax.set_xticks(mix_clean["year"].tolist())
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_fluent_overall_three_lines")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — COHORT ANALYSIS BY EL STATUS (charts 41–44)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 7: Cohort analysis by EL status…")

hb_el_coh = hb(SG_EL).dropna(subset=["pct_met_and_above", "cohort_year"]).copy()
ca_el_coh = ca(SG_EL).dropna(subset=["pct_met_and_above", "cohort_year"]).copy()
hb_fl_coh = hb(SG_FLUENT).dropna(subset=["pct_met_and_above", "cohort_year"]).copy()
ca_fl_coh = ca(SG_FLUENT).dropna(subset=["pct_met_and_above", "cohort_year"]).copy()

el_cohorts = sorted(hb_el_coh["cohort_year"].unique())
fl_cohorts = sorted(hb_fl_coh["cohort_year"].unique())

# 41. EL cohort spaghetti — Healdsburg
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(el_cohorts):
    d = hb_el_coh[hb_el_coh["cohort_year"] == cy].sort_values("grade")
    if len(d) < 2:
        continue
    ax.plot(d["grade"], d["pct_met_and_above"], "o-",
            color=COHORT_COLORS[i % len(COHORT_COLORS)], lw=1.8, alpha=0.85,
            label=f"Cohort {int(cy)}")
ax.set_title("EL Student Cohort Progression — Healdsburg Math\n(Each line = EL students who entered 3rd grade in that year)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_el_spaghetti_hb")

# 42. Fluent cohort spaghetti — Healdsburg
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(fl_cohorts):
    d = hb_fl_coh[hb_fl_coh["cohort_year"] == cy].sort_values("grade")
    if len(d) < 2:
        continue
    ax.plot(d["grade"], d["pct_met_and_above"], "s-",
            color=COHORT_COLORS[i % len(COHORT_COLORS)], lw=1.8, alpha=0.85,
            label=f"Cohort {int(cy)}")
ax.set_title("Fluent-English Student Cohort Progression — Healdsburg Math\n(Each line = IFEP+RFEP+EO students who entered 3rd grade in that year)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_fluent_spaghetti_hb")

# 43. EL cohort: Healdsburg vs CA
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(el_cohorts):
    col = COHORT_COLORS[i % len(COHORT_COLORS)]
    d_hb = hb_el_coh[hb_el_coh["cohort_year"] == cy].sort_values("grade")
    d_ca = ca_el_coh[ca_el_coh["cohort_year"] == cy].sort_values("grade")
    if len(d_hb) < 2:
        continue
    ax.plot(d_hb["grade"], d_hb["pct_met_and_above"], "o-", color=col, lw=2, alpha=0.9,
            label=f"HB EL {int(cy)}")
    ax.plot(d_ca["grade"], d_ca["pct_met_and_above"], "x--", color=col, lw=1.2, alpha=0.5)
ax.set_title("EL Cohort Progression: Healdsburg (solid) vs California (dashed)\n(Same color = same cohort — how HB EL students compare to CA EL)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=7, ncol=2)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_el_hb_vs_ca")

# 44. Fluent cohort: Healdsburg vs CA
fig, ax = plt.subplots(figsize=(10, 6))
for i, cy in enumerate(fl_cohorts):
    col = COHORT_COLORS[i % len(COHORT_COLORS)]
    d_hb = hb_fl_coh[hb_fl_coh["cohort_year"] == cy].sort_values("grade")
    d_ca = ca_fl_coh[ca_fl_coh["cohort_year"] == cy].sort_values("grade")
    if len(d_hb) < 2:
        continue
    ax.plot(d_hb["grade"], d_hb["pct_met_and_above"], "s-", color=col, lw=2, alpha=0.9,
            label=f"HB Fluent {int(cy)}")
    ax.plot(d_ca["grade"], d_ca["pct_met_and_above"], "x--", color=col, lw=1.2, alpha=0.5)
ax.set_title("Fluent-English Cohort Progression: Healdsburg (solid) vs California (dashed)\n(Same color = same cohort; HB Fluent vs CA Fluent)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=7, ncol=2)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_fluent_hb_vs_ca")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — HEAT MAPS (charts 45–51)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 8: Heat maps…")

def make_heatmap_matrix(df, group_name):
    """Build grade x year matrix for a student group."""
    d = df[df["student_group_name"] == group_name][
        ["year", "grade", "pct_met_and_above"]
    ].dropna()
    pivot = d.pivot_table(index="grade", columns="year", values="pct_met_and_above",
                          aggfunc="mean")
    pivot = pivot.reindex(index=GRADES, columns=ALL_YEARS)
    return pivot

def draw_heatmap(pivot, title, ax, vmin=None, vmax=None, cmap="RdYlGn",
                 center=None, fmt=".0f"):
    import matplotlib.colors as mcolors
    if vmin is None:
        vmin = np.nanmin(pivot.values)
    if vmax is None:
        vmax = np.nanmax(pivot.values)
    if center is not None:
        norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=center, vmax=vmax)
        im = ax.imshow(pivot.values, cmap=cmap, norm=norm, aspect="auto")
    else:
        im = ax.imshow(pivot.values, cmap=cmap, vmin=vmin, vmax=vmax, aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=90, fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([f"Gr {g}" for g in pivot.index], fontsize=9)
    ax.set_xlabel("Year")
    ax.set_ylabel("Grade")
    for r in range(pivot.values.shape[0]):
        for c in range(pivot.values.shape[1]):
            v = pivot.values[r, c]
            if np.isfinite(v):
                ax.text(c, r, f"{v:{fmt}}%", ha="center", va="center",
                        fontsize=7, color="black" if 20 < v < 80 else "white")
    return im

# 45. Healdsburg all students heat map
hb_pivot = make_heatmap_matrix(HB, SG_ALL)
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(hb_pivot, "", ax, vmin=0, vmax=100)
ax.set_title("Healdsburg Unified Math Performance — All Students\n(% Meeting or Exceeding Standards by Grade and Year)")
plt.colorbar(im, ax=ax, label="% Meeting or Exceeding Standards")
fig.tight_layout()
save(fig, "heatmap_hb_all_students")

# 46. CA all students heat map
ca_pivot = make_heatmap_matrix(CA, SG_ALL)
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(ca_pivot, "", ax, vmin=0, vmax=100)
ax.set_title("California Math Performance — All Students\n(% Meeting or Exceeding Standards by Grade and Year)")
plt.colorbar(im, ax=ax, label="% Meeting or Exceeding Standards")
fig.tight_layout()
save(fig, "heatmap_ca_all_students")

# 47. Healdsburg minus CA gap heat map
gap_pivot = hb_pivot - ca_pivot
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(gap_pivot, "", ax, vmin=-25, vmax=25, cmap="RdBu",
                  center=0, fmt="+.0f")
ax.set_title("Healdsburg vs California Math Gap — All Students\n(Percentage points: Healdsburg minus CA; blue = HB above, red = below)")
plt.colorbar(im, ax=ax, label="Healdsburg − California (pp)")
fig.tight_layout()
save(fig, "heatmap_gap_hb_minus_ca")

# 48. EL heat map — Healdsburg
hb_el_pivot = make_heatmap_matrix(HB, SG_EL)
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(hb_el_pivot, "", ax, vmin=0, vmax=100)
ax.set_title("Healdsburg EL Student Math Performance\n(% Meeting or Exceeding Standards by Grade and Year; blank = suppressed)")
plt.colorbar(im, ax=ax, label="% Meeting or Exceeding Standards")
fig.tight_layout()
save(fig, "heatmap_hb_el_students")

# 49. Fluent heat map — Healdsburg
hb_fl_pivot = make_heatmap_matrix(HB, SG_FLUENT)
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(hb_fl_pivot, "", ax, vmin=0, vmax=100)
ax.set_title("Healdsburg Fluent-English Student Math Performance\n(IFEP+RFEP+EO; % Meeting or Exceeding Standards by Grade and Year)")
plt.colorbar(im, ax=ax, label="% Meeting or Exceeding Standards")
fig.tight_layout()
save(fig, "heatmap_hb_fluent_students")

# 50. EL heat map — Healdsburg minus CA EL
ca_el_pivot = make_heatmap_matrix(CA, SG_EL)
el_gap_pivot = hb_el_pivot - ca_el_pivot
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(el_gap_pivot, "", ax, vmin=-25, vmax=25, cmap="RdBu",
                  center=0, fmt="+.0f")
ax.set_title("Healdsburg EL vs California EL Math Gap\n(pp: Healdsburg EL minus CA EL; blue = HB EL above, red = below)")
plt.colorbar(im, ax=ax, label="Healdsburg EL − California EL (pp)")
fig.tight_layout()
save(fig, "heatmap_el_gap_hb_minus_ca")

# 51. Fluent gap heat map — Healdsburg Fluent minus CA Fluent
ca_fl_pivot = make_heatmap_matrix(CA, SG_FLUENT)
fl_gap_pivot = hb_fl_pivot - ca_fl_pivot
fig, ax = plt.subplots(figsize=(12, 5))
im = draw_heatmap(fl_gap_pivot, "", ax, vmin=-25, vmax=25, cmap="RdBu",
                  center=0, fmt="+.0f")
ax.set_title("Healdsburg Fluent vs California Fluent Math Gap\n(pp: HB IFEP+RFEP+EO minus CA IFEP+RFEP+EO; blue = HB above)")
plt.colorbar(im, ax=ax, label="Healdsburg Fluent − California Fluent (pp)")
fig.tight_layout()
save(fig, "heatmap_fluent_gap_hb_minus_ca")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — ADDITIONAL ANALYSIS (charts 52–62)
# ══════════════════════════════════════════════════════════════════════════════
print("Section 9: Additional analysis…")

# 52. 4-panel summary dashboard
fig = plt.figure(figsize=(14, 10))
fig.suptitle("Healdsburg Unified Math Performance — Summary Dashboard", fontsize=14, y=1.01)
gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.4, wspace=0.35)

ax1 = fig.add_subplot(gs[0, 0])
ax2 = fig.add_subplot(gs[0, 1])
ax3 = fig.add_subplot(gs[1, 0])
ax4 = fig.add_subplot(gs[1, 1])

# Panel 1: EL% over time
ax1.bar(mix["year"], mix["el_pct"], color=C_EL, alpha=0.75)
ax1.set_title("EL Share Over Time", fontsize=10)
ax1.set_ylabel("% of Students who are EL")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax1.set_xticks(mix["year"].tolist())
ax1.set_xticklabels(mix["year"].tolist(), rotation=90, fontsize=7)
ax1.grid(axis="y", alpha=0.3)

# Panel 2: EL performance
hx, hy = series_xy(hb_el_ann, "year", "pct_met_and_above")
cx, cy_ = series_xy(ca_el_ann, "year", "pct_met_and_above")
ax2.plot(hx, hy, "o-", color=C_HB_EL, lw=2, label="Healdsburg EL")
ax2.plot(cx, cy_, "s--", color=C_CA_EL, lw=2, label="CA EL")
ax2.set_title("EL Math Performance", fontsize=10)
ax2.set_ylabel("% Meeting or Exceeding Standards")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax2.set_xticks(ALL_YEARS)
ax2.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
ax2.legend(fontsize=7)
ax2.grid(axis="y", alpha=0.3)

# Panel 3: Fluent performance
hx, hy = series_xy(hb_fl_ann, "year", "pct_met_and_above")
cx, cy_ = series_xy(ca_fl_ann, "year", "pct_met_and_above")
ax3.plot(hx, hy, "o-", color=C_HB_FL, lw=2, label="Healdsburg Fluent")
ax3.plot(cx, cy_, "s--", color=C_CA_FL, lw=2, label="CA Fluent")
ax3.set_title("Fluent-English Math Performance", fontsize=10)
ax3.set_ylabel("% Meeting or Exceeding Standards")
ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax3.set_xticks(ALL_YEARS)
ax3.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
ax3.legend(fontsize=7)
ax3.grid(axis="y", alpha=0.3)

# Panel 4: Overall performance
hx, hy = series_xy(hb_all_ann, "year", "pct_met_and_above")
cx, cy_ = series_xy(ca_all_ann, "year", "pct_met_and_above")
ax4.plot(hx, hy, "o-", color=C_HB, lw=2, label="Healdsburg All")
ax4.plot(cx, cy_, "s--", color=C_CA, lw=2, label="CA All")
ax4.set_title("Overall Math Performance", fontsize=10)
ax4.set_ylabel("% Meeting or Exceeding Standards")
ax4.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax4.set_xticks(ALL_YEARS)
ax4.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
ax4.legend(fontsize=7)
ax4.grid(axis="y", alpha=0.3)

fig.tight_layout()
save(fig, "summary_dashboard_4panel")

# 53. Dot plot: performance by grade for each year (Healdsburg)
fig, ax = plt.subplots(figsize=(11, 6))
cmap_yr = plt.cm.viridis(np.linspace(0, 1, len(ALL_YEARS)))
for i, yr in enumerate(ALL_YEARS):
    d = hb_ga[hb_ga["year"] == yr].sort_values("grade")
    ax.scatter(d["grade"], d["pct_met_and_above"], color=cmap_yr[i], s=90, zorder=5,
               label=str(yr), alpha=0.9)
    ax.plot(d["grade"], d["pct_met_and_above"], color=cmap_yr[i], lw=1, alpha=0.5)
ax.set_title("Math Performance Profile Across Grades by Year — Healdsburg Unified\n(All Students; each colored line = one year's grade profile)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8, title="Year")
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "dot_plot_grade_profile_by_year")

# 54. RFEP performance over time: Healdsburg vs CA
hb_rfep_ann = hb_annual(SG_RFEP)
ca_rfep_ann = ca_annual(SG_RFEP)
if not hb_rfep_ann.empty:
    fig, ax = plt.subplots(figsize=(9, 5))
    hx, hy = series_xy(hb_rfep_ann, "year", "pct_met_and_above")
    cx, cy_ = series_xy(ca_rfep_ann, "year", "pct_met_and_above")
    ax.plot(hx, hy, "o-", color=C_HB, lw=2.5, label="Healdsburg RFEP")
    ax.plot(cx, cy_, "s--", color=C_CA, lw=2.5, label="California RFEP")
    for x, y in zip(hx, hy):
        ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=7, color=C_HB)
    ax.set_title("RFEP Math Performance Over Time: Healdsburg vs California\n(Reclassified Fluent English Proficient — all grades weighted avg)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, "rfep_overall_line_hb_vs_ca")

# 55. EO performance over time: Healdsburg vs CA
hb_eo_ann = hb_annual(SG_EO)
ca_eo_ann = ca_annual(SG_EO)
if not hb_eo_ann.empty:
    fig, ax = plt.subplots(figsize=(9, 5))
    hx, hy = series_xy(hb_eo_ann, "year", "pct_met_and_above")
    cx, cy_ = series_xy(ca_eo_ann, "year", "pct_met_and_above")
    ax.plot(hx, hy, "o-", color=C_HB, lw=2.5, label="Healdsburg EO")
    ax.plot(cx, cy_, "s--", color=C_CA, lw=2.5, label="California EO")
    for x, y in zip(hx, hy):
        ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=7, color=C_HB)
    ax.set_title("English-Only (EO) Math Performance Over Time: Healdsburg vs California\n(Students who have always been English-only; all grades weighted avg)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, "eo_overall_line_hb_vs_ca")

# 56. EverEL vs NeverEL performance: Healdsburg vs CA
hb_ever_ann = hb_annual(SG_EVEREL)
ca_ever_ann = ca_annual(SG_EVEREL)
hb_never_ann = hb_annual(SG_NEVEREL)
ca_never_ann = ca_annual(SG_NEVEREL)

if not hb_ever_ann.empty or not hb_never_ann.empty:
    fig, ax = plt.subplots(figsize=(9, 5))
    if not hb_ever_ann.empty:
        hx, hy = series_xy(hb_ever_ann, "year", "pct_met_and_above")
        ax.plot(hx, hy, "o-", color="#8e44ad", lw=2.5, label="Healdsburg EverEL")
    if not ca_ever_ann.empty:
        cx, cy_ = series_xy(ca_ever_ann, "year", "pct_met_and_above")
        ax.plot(cx, cy_, "o--", color="#d7bde2", lw=2, label="CA EverEL")
    if not hb_never_ann.empty:
        hx2, hy2 = series_xy(hb_never_ann, "year", "pct_met_and_above")
        ax.plot(hx2, hy2, "s-", color="#1a5276", lw=2.5, label="Healdsburg NeverEL")
    if not ca_never_ann.empty:
        cx2, cy2 = series_xy(ca_never_ann, "year", "pct_met_and_above")
        ax.plot(cx2, cy2, "s--", color="#5dade2", lw=2, label="CA NeverEL")
    ax.set_title("EverEL vs NeverEL Math Performance: Healdsburg vs California\n(EverEL = currently or previously EL; NeverEL = always English-proficient)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, "everel_vs_neverel_hb_vs_ca")

# 57. LTEL performance over time — Healdsburg
hb_ltel_ann = hb_annual(SG_LTEL)
ca_ltel_ann = ca_annual(SG_LTEL)
if not hb_ltel_ann.empty:
    fig, ax = plt.subplots(figsize=(9, 5))
    hx, hy = series_xy(hb_ltel_ann, "year", "pct_met_and_above")
    cx, cy_ = series_xy(ca_ltel_ann, "year", "pct_met_and_above")
    ax.plot(hx, hy, "o-", color=C_HB, lw=2.5, label="Healdsburg LTEL")
    ax.plot(cx, cy_, "s--", color=C_CA, lw=2.5, label="California LTEL")
    ax.set_title("Long-Term English Learner (LTEL) Math Performance: Healdsburg vs CA\n(Students EL for 6+ years — all grades weighted avg)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, "ltel_overall_line_hb_vs_ca")

# 58. Grade 3 cohort entry performance vs eventual grade 8 outcome — scatter
entries, outcomes, cys_58 = [], [], []
for cy in cohort_years:
    d = hb_cohort_all[hb_cohort_all["cohort_year"] == cy]
    g3 = d[d["grade"] == 3]["pct_met_and_above"].values
    g8 = d[d["grade"] == 8]["pct_met_and_above"].values
    if len(g3) and len(g8) and np.isfinite(g3[0]) and np.isfinite(g8[0]):
        entries.append(g3[0])
        outcomes.append(g8[0])
        cys_58.append(int(cy))

if len(entries) >= 3:
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(entries, outcomes, c=cys_58, cmap="viridis", s=120, zorder=5)
    for cy, e, o in zip(cys_58, entries, outcomes):
        ax.annotate(f"Cohort {cy}\n(Gr3={e:.0f}%, Gr8={o:.0f}%)",
                    (e, o), textcoords="offset points", xytext=(6, 4), fontsize=7)
    z = np.polyfit(entries, outcomes, 1)
    xs = np.linspace(min(entries), max(entries), 50)
    ax.plot(xs, np.poly1d(z)(xs), "r--", lw=1.5, alpha=0.6)
    ax.plot([0, 100], [0, 100], "gray", lw=0.8, ls=":", label="y = x (no change)")
    ax.set_xlabel("Grade 3 Performance (% Meeting Standards)")
    ax.set_ylabel("Grade 8 Performance (% Meeting Standards)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    pct_yaxis(ax)
    ax.set_title("Does Grade 3 Performance Predict Grade 8? — Healdsburg Cohorts\n(Each dot = one cohort; above diagonal = improved from Gr3 to Gr8)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    save(fig, "scatter_gr3_entry_vs_gr8_outcome")

# 59. EL vs Fluent performance gap by grade (averaged across years)
def avg_perf_by_grade(df_in, sg):
    d = df_in[df_in["student_group_name"] == sg]
    return d.groupby("grade")["pct_met_and_above"].mean()

hb_el_by_grade = avg_perf_by_grade(HB, SG_EL)
hb_fl_by_grade = avg_perf_by_grade(HB, SG_FLUENT)
ca_el_by_grade = avg_perf_by_grade(CA, SG_EL)
ca_fl_by_grade = avg_perf_by_grade(CA, SG_FLUENT)

fig, ax = plt.subplots(figsize=(9, 5))
x = np.arange(len(GRADES))
w = 0.22
ax.bar(x - 1.5*w, [hb_el_by_grade.get(g, np.nan) for g in GRADES], w,
       color=C_HB_EL, label="Healdsburg EL", alpha=0.85)
ax.bar(x - 0.5*w, [ca_el_by_grade.get(g, np.nan) for g in GRADES], w,
       color=C_CA_EL, label="CA EL", alpha=0.85)
ax.bar(x + 0.5*w, [hb_fl_by_grade.get(g, np.nan) for g in GRADES], w,
       color=C_HB_FL, label="Healdsburg Fluent", alpha=0.85)
ax.bar(x + 1.5*w, [ca_fl_by_grade.get(g, np.nan) for g in GRADES], w,
       color=C_CA_FL, label="CA Fluent", alpha=0.85)
ax.set_xticks(x)
ax.set_xticklabels([f"Grade {g}" for g in GRADES])
ax.set_title("Average Math Performance by Grade: EL vs Fluent, Healdsburg vs CA\n(Averaged across all available years; 4 bars per grade)")
ax.set_ylabel("Avg % Meeting or Exceeding Standards")
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "avg_perf_by_grade_el_fluent_4bars")

# 60. Healdsburg EL gap from CA EL by grade over time (facet)
fig, axes = plt.subplots(2, 4, figsize=(18, 9), sharey=True)
fig.suptitle("Healdsburg EL vs California EL Gap by Grade Over Time\n(+ = HB EL outperforms CA EL; bars per year)", fontsize=13, y=1.01)
ax_flat = axes.flatten()
all_gaps_60 = []
for grade in GRADES:
    hb_g = hb_el_ga[hb_el_ga["grade"] == grade].set_index("year")["pct_met_and_above"]
    ca_g = ca_el_ga[ca_el_ga["grade"] == grade].set_index("year")["pct_met_and_above"]
    gap = (hb_g - ca_g).dropna()
    all_gaps_60.extend(gap.tolist())

yl60_lo = min(all_gaps_60) - 3 if all_gaps_60 else -15
yl60_hi = max(all_gaps_60) + 3 if all_gaps_60 else 15

for i, grade in enumerate(GRADES):
    ax = ax_flat[i]
    hb_g = hb_el_ga[hb_el_ga["grade"] == grade].set_index("year")["pct_met_and_above"]
    ca_g = ca_el_ga[ca_el_ga["grade"] == grade].set_index("year")["pct_met_and_above"]
    gap = (hb_g - ca_g).dropna()
    colors_60 = [C_HB if v >= 0 else "#e74c3c" for v in gap.values]
    ax.bar(gap.index, gap.values, color=colors_60, alpha=0.8)
    ax.axhline(0, color="black", lw=0.8)
    ax.set_title(f"Grade {grade}", fontsize=11)
    ax.set_xticks(ALL_YEARS)
    ax.set_xticklabels(ALL_YEARS, rotation=90, fontsize=7)
    ax.set_ylim(yl60_lo, yl60_hi)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.0f}"))
    ax.set_ylabel("Gap (pp)")
    ax.set_xlabel("Year")
    ax.grid(axis="y", alpha=0.3)

ax_flat[-1].set_visible(False)
fig.tight_layout()
save(fig, "facet_el_gap_hb_vs_ca_by_grade")

# 61. EL vs Fluent cohort: comparison on same chart (Healdsburg only)
fig, ax = plt.subplots(figsize=(10, 6))
common_cohorts = sorted(set(hb_el_coh["cohort_year"]) & set(hb_fl_coh["cohort_year"]))
for i, cy in enumerate(common_cohorts):
    col = COHORT_COLORS[i % len(COHORT_COLORS)]
    d_el = hb_el_coh[hb_el_coh["cohort_year"] == cy].sort_values("grade")
    d_fl = hb_fl_coh[hb_fl_coh["cohort_year"] == cy].sort_values("grade")
    if len(d_el) >= 2:
        ax.plot(d_el["grade"], d_el["pct_met_and_above"], "o-", color=col,
                lw=2, alpha=0.9, label=f"EL cohort {int(cy)}")
    if len(d_fl) >= 2:
        ax.plot(d_fl["grade"], d_fl["pct_met_and_above"], "s--", color=col,
                lw=1.5, alpha=0.6)
ax.set_title("EL (solid) vs Fluent (dashed) Cohort Progression — Healdsburg Math\n(Same color = same cohort year; gap = EL achievement gap)")
ax.set_xlabel("Grade")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(GRADES)
pct_yaxis(ax)
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=7)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "cohort_el_vs_fluent_same_chart")

# 62. RFEP vs EL vs EO: three subgroups over time — Healdsburg
hb_rfep_ann2 = hb_annual(SG_RFEP)
hb_eo_ann2   = hb_annual(SG_EO)
hb_el_ann2   = hb_el_ann.copy()

fig, ax = plt.subplots(figsize=(10, 5))
if not hb_el_ann2.empty:
    hx, hy = series_xy(hb_el_ann2, "year", "pct_met_and_above")
    ax.plot(hx, hy, "o-", color=C_EL, lw=2.5, label="EL (excl. RFEP)")
if not hb_rfep_ann2.empty:
    hx, hy = series_xy(hb_rfep_ann2, "year", "pct_met_and_above")
    ax.plot(hx, hy, "^-", color="#8e44ad", lw=2.5, label="RFEP (reclassified EL)")
if not hb_eo_ann2.empty:
    hx, hy = series_xy(hb_eo_ann2, "year", "pct_met_and_above")
    ax.plot(hx, hy, "s-", color="#1a5276", lw=2.5, label="EO (English only)")
hx, hy = series_xy(hb_all_ann, "year", "pct_met_and_above")
ax.plot(hx, hy, "D--", color="gray", lw=1.5, label="All Students", alpha=0.7)

ax.set_title("Math Performance by English Fluency Subgroup — Healdsburg Unified\n(EL vs RFEP vs EO vs All Students; shows reclassification 'premium')")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(ALL_YEARS)
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "el_rfep_eo_all_comparison_hb")

# 63. Healdsburg EL performance relative to CA overall (not CA EL)
hb_el_ann3 = hb_el_ann.copy()
ca_all_ann2 = ca_all_ann.copy()

fig, ax = plt.subplots(figsize=(9, 5))
hx, hy = series_xy(hb_el_ann3, "year", "pct_met_and_above")
cx, cy_ = series_xy(ca_all_ann2, "year", "pct_met_and_above")
ax.plot(hx, hy, "o-", color=C_EL, lw=2.5, label="Healdsburg EL students")
ax.plot(cx, cy_, "s--", color=C_CA, lw=2.5, label="California ALL students")
ax.fill_between(
    [yr for yr in hx if yr in cx],
    [hy[hx.index(yr)] for yr in hx if yr in cx],
    [cy_[cx.index(yr)] for yr in cx if yr in hx],
    alpha=0.12, color=C_EL, label="EL gap from CA overall"
)
ax.set_title("Healdsburg EL Performance vs California Overall — Math\n(How far are Healdsburg EL students from the CA state average?)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(ALL_YEARS)
pct_yaxis(ax)
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "hb_el_vs_ca_overall_gap")

# 64. Percent of EL students enrolled 12 months or more vs less — performance
SG_EL_12 = "ELs enrolled 12 months or more"
SG_EL_L12 = "ELs enrolled less than 12 months"
hb_el12_ann = hb_annual(SG_EL_12)
hb_ell12_ann = hb_annual(SG_EL_L12)

if not hb_el12_ann.empty:
    fig, ax = plt.subplots(figsize=(9, 5))
    hx, hy = series_xy(hb_el12_ann, "year", "pct_met_and_above")
    ax.plot(hx, hy, "o-", color="#e67e22", lw=2.5, label="ELs enrolled 12+ months")
    if not hb_ell12_ann.empty:
        hx2, hy2 = series_xy(hb_ell12_ann, "year", "pct_met_and_above")
        ax.plot(hx2, hy2, "s--", color="#c0392b", lw=2.5, label="ELs enrolled <12 months")
    hx3, hy3 = series_xy(hb_el_ann, "year", "pct_met_and_above")
    ax.plot(hx3, hy3, "^-", color=C_EL, lw=2, alpha=0.6, label="All ELs")
    ax.set_title("EL Math Performance by Length of Enrollment — Healdsburg\n(Longer-enrolled ELs tend to have better outcomes)")
    ax.set_xlabel("Year")
    ax.set_ylabel("% Meeting or Exceeding Standards")
    ax.set_xticks(ALL_YEARS)
    pct_yaxis(ax)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    save(fig, "el_enrollment_length_hb")

# 65. Overall trend: Healdsburg all 4 English-fluency groups + CA overall
fig, ax = plt.subplots(figsize=(11, 6))
lines_info = [
    (hb_el_ann,   "o-",  C_EL,      "HB: EL"),
    (hb_fl_ann,   "s-",  C_FLUENT,  "HB: Fluent (IFEP+RFEP+EO)"),
    (hb_rfep_ann, "^-",  "#8e44ad", "HB: RFEP only"),
    (hb_all_ann,  "D-",  C_HB,      "HB: All Students"),
    (ca_all_ann,  "x--", C_CA,      "CA: All Students"),
]
for df_in, marker, color, label in lines_info:
    if not df_in.empty:
        hx, hy = series_xy(df_in, "year", "pct_met_and_above")
        ax.plot(hx, hy, marker, color=color, lw=2, label=label, alpha=0.9)
ax.set_title("Math Performance — All English-Fluency Groups vs CA Overall: Healdsburg\n(Comprehensive view of all English fluency groups)")
ax.set_xlabel("Year")
ax.set_ylabel("% Meeting or Exceeding Standards")
ax.set_xticks(ALL_YEARS)
pct_yaxis(ax)
ax.legend(loc="upper left", fontsize=8)
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
save(fig, "all_fluency_groups_plus_ca_overview")

# 66. EL percentage change and performance change: combined story chart
mix2 = mix.copy().sort_values("year")
fig, axes = plt.subplots(3, 1, figsize=(10, 12), sharex=True)
fig.suptitle("The Full Story: EL Share, EL Performance, and Overall Performance\n— Healdsburg Unified Math", fontsize=13)

axes[0].bar(mix2["year"], mix2["el_pct"], color=C_EL, alpha=0.75)
axes[0].set_ylabel("EL % of Students Tested")
axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
axes[0].set_title("(A) EL Share Over Time", fontsize=10)
axes[0].grid(axis="y", alpha=0.3)

axes[1].plot(mix2["year"], mix2["el_perf"], "o-", color=C_EL, lw=2.5, label="EL")
axes[1].plot(mix2["year"], mix2["fluent_perf"], "s-", color=C_FLUENT, lw=2.5, label="Fluent")
axes[1].set_ylabel("% Meeting or Exceeding Standards")
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
axes[1].set_title("(B) EL vs Fluent Performance Over Time", fontsize=10)
axes[1].legend(fontsize=8)
axes[1].grid(axis="y", alpha=0.3)

axes[2].plot(mix2["year"], mix2["overall_perf"], "o-", color=C_HB, lw=2.5,
             label="Healdsburg Overall")
ca_all_s = ca_all_ann.set_index("year")["pct_met_and_above"]
ca_yrs = [y for y in mix2["year"] if y in ca_all_s.index]
ca_vals = [ca_all_s[y] for y in ca_yrs]
axes[2].plot(ca_yrs, ca_vals, "s--", color=C_CA, lw=2.5, label="CA Overall")
axes[2].set_ylabel("% Meeting or Exceeding Standards")
axes[2].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
axes[2].set_title("(C) Overall Math Performance vs California", fontsize=10)
axes[2].legend(fontsize=8)
axes[2].grid(axis="y", alpha=0.3)
axes[2].set_xlabel("Year")
axes[2].set_xticks(mix2["year"].tolist())

fig.tight_layout()
save(fig, "full_story_3panel_el_share_perf_overall")

# ── Cleanup ───────────────────────────────────────────────────────────────────
# Remove exploration helper file
import os
try:
    os.remove("explore-hb.py")
except FileNotFoundError:
    pass

print()
print(f"Done! Generated {_n[0]} charts in {OUTPUT_DIR}/")
