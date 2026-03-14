#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "gradio>=4.0",
#   "pandas>=2.0",
#   "pyarrow>=14.0",
#   "matplotlib>=3.7",
# ]
# ///
"""
CAASPP SBAC Score Analysis - Gradio interface

Features:
- Filter by entity (State, District, School) and student group
- Filter by test (ELA or Math)
- Cohort facet grid: one chart per cohort, x=year, bars=pct met and above
- Grade facet grid: one chart per grade, x=year, bars=pct met and above

Both grids show: selected entity (red) vs California state (blue).
"""

from pathlib import Path

import gradio as gr
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

DATA_PATH = Path(__file__).parent / "data" / "sbac_data.parquet"

ALL_YEARS = [2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024, 2025]
CHART_COLS = 3
COLOR_STATE = "#4878CF"    # blue
COLOR_SELECTED = "#D65F5F"  # red

# type_id values
TYPE_STATE = 4
TYPE_COUNTY = 5
TYPE_DISTRICT = 6
TYPE_SCHOOL = [7, 9]  # 9 = some charter/other school types


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    return df


# ── cached globals ─────────────────────────────────────────────────────────────
_df: pd.DataFrame | None = None


def get_df() -> pd.DataFrame:
    global _df
    if _df is None:
        _df = load_data()
    return _df


def get_district_choices() -> list[str]:
    df = get_df()
    districts = (
        df[df["type_id"] == TYPE_DISTRICT][["district_name", "county_name"]]
        .drop_duplicates()
        .dropna(subset=["district_name"])
    )
    districts = districts[districts["district_name"].str.strip() != ""]
    labels = (
        districts.apply(
            lambda r: f"{r['district_name']} ({r['county_name']})"
            if pd.notna(r["county_name"]) and r["county_name"].strip()
            else r["district_name"],
            axis=1,
        )
        .sort_values()
        .tolist()
    )
    return labels


def get_school_choices() -> list[str]:
    df = get_df()
    schools = (
        df[df["type_id"].isin(TYPE_SCHOOL)][["school_name", "district_name", "county_name"]]
        .drop_duplicates()
        .dropna(subset=["school_name"])
    )
    schools = schools[schools["school_name"].str.strip() != ""]
    labels = (
        schools.apply(
            lambda r: f"{r['school_name']} — {r['district_name']} ({r['county_name']})"
            if pd.notna(r["district_name"]) and r["district_name"].strip()
            else r["school_name"],
            axis=1,
        )
        .sort_values()
        .tolist()
    )
    return labels


def get_student_group_choices() -> list[tuple[str, int]]:
    df = get_df()
    groups = (
        df[["student_group_id", "student_group_name", "student_group_category"]]
        .drop_duplicates(subset=["student_group_id"])
        .dropna(subset=["student_group_name"])
        .sort_values(["student_group_category", "student_group_name"])
    )
    choices = []
    for _, row in groups.iterrows():
        label = f"{row['student_group_name']}"
        choices.append((label, int(row["student_group_id"])))
    return choices


def select_entity_rows(df: pd.DataFrame, entity_type: str, entity_label: str) -> pd.DataFrame:
    """Return rows matching the selected entity."""
    if entity_type == "State":
        return df[df["type_id"] == TYPE_STATE]
    elif entity_type == "District":
        district_name = entity_label.split(" (")[0].strip()
        mask = (df["type_id"] == TYPE_DISTRICT) & (df["district_name"] == district_name)
        if " (" in entity_label:
            county_part = entity_label.split(" (")[1].rstrip(")")
            mask &= df["county_name"] == county_part
        return df[mask]
    elif entity_type == "School":
        school_name = entity_label.split(" — ")[0].strip()
        mask = df["type_id"].isin(TYPE_SCHOOL) & (df["school_name"] == school_name)
        if " — " in entity_label:
            rest = entity_label.split(" — ")[1]
            district_name = rest.split(" (")[0].strip()
            mask &= df["district_name"] == district_name
        return df[mask]
    return df.iloc[0:0]


def _draw_bar_cell(
    ax,
    years: list[int],
    state_vals: dict[int, float],
    selected_vals: dict[int, float],
    title: str,
    is_state_selected: bool,
    show_legend: bool,
):
    """Draw grouped bars into a single matplotlib Axes."""
    import numpy as np

    x = list(range(len(years)))
    y_state = [state_vals.get(y) for y in years]
    y_sel = [selected_vals.get(y) for y in years]

    if is_state_selected:
        bars = ax.bar(x, y_state, color=COLOR_STATE, label="California", width=0.6)
    else:
        w = 0.38
        offsets = [-w / 2, w / 2]
        b1 = ax.bar([xi + offsets[0] for xi in x], y_state, width=w,
                    color=COLOR_STATE, label="California")
        b2 = ax.bar([xi + offsets[1] for xi in x], y_sel, width=w,
                    color=COLOR_SELECTED, label="Selected")

    ax.set_xticks(x)
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=7)
    ax.set_ylim(0, 105)
    ax.set_yticks([0, 20, 40, 60, 80, 100])
    ax.set_yticklabels(["0%", "20%", "40%", "60%", "80%", "100%"], fontsize=7)
    ax.set_title(title, fontsize=9, pad=4)
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    if show_legend and not is_state_selected:
        ax.legend(fontsize=7, loc="upper left")


def build_cohort_grid(
    state_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    is_state_selected: bool,
) -> plt.Figure:
    """Build cohort facet grid: one chart per cohort, x=year."""
    all_cohorts = sorted(
        set(state_df["cohort_year"].dropna().unique().tolist())
        | set(selected_df["cohort_year"].dropna().unique().tolist() if not is_state_selected else [])
    )

    def cohort_count(cy):
        return len(state_df[state_df["cohort_year"] == cy]["year"].unique())

    all_cohorts = [cy for cy in all_cohorts if cohort_count(cy) >= 2]

    if not all_cohorts:
        fig, ax = plt.subplots(figsize=(6, 2))
        ax.text(0.5, 0.5, "No cohort data available", ha="center", va="center",
                transform=ax.transAxes)
        ax.axis("off")
        return fig

    n_cohorts = len(all_cohorts)
    n_rows = (n_cohorts + CHART_COLS - 1) // CHART_COLS
    fig, axes = plt.subplots(n_rows, CHART_COLS,
                              figsize=(CHART_COLS * 4.5, n_rows * 3.2),
                              squeeze=False)

    for idx, cohort_year in enumerate(all_cohorts):
        r, c = divmod(idx, CHART_COLS)
        ax = axes[r][c]
        show_legend = idx == 0

        coh_state = state_df[state_df["cohort_year"] == cohort_year]
        coh_sel = selected_df[selected_df["cohort_year"] == cohort_year] if not is_state_selected else coh_state

        cohort_years = sorted(
            set(coh_state["year"].dropna().tolist())
            | set(coh_sel["year"].dropna().tolist())
        )
        cohort_years = [y for y in ALL_YEARS if y in cohort_years]

        state_vals = coh_state.groupby("year")["pct_met_and_above"].mean().to_dict()
        sel_vals = coh_sel.groupby("year")["pct_met_and_above"].mean().to_dict() if not is_state_selected else state_vals

        _draw_bar_cell(ax, cohort_years, state_vals, sel_vals,
                       f"Cohort {int(cohort_year)}", is_state_selected, show_legend)

    # Hide unused axes
    for idx in range(n_cohorts, n_rows * CHART_COLS):
        r, c = divmod(idx, CHART_COLS)
        axes[r][c].axis("off")

    fig.suptitle("Performance by Student Cohort (% Met or Exceeded Standard)",
                 fontsize=12, y=1.01)
    fig.tight_layout()
    return fig


def build_grade_grid(
    state_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    is_state_selected: bool,
) -> plt.Figure:
    """Build grade facet grid: one chart per grade, x=year."""
    grades = sorted(
        set(state_df["grade"].dropna().unique().tolist())
        | set(selected_df["grade"].dropna().unique().tolist() if not is_state_selected else [])
    )

    if not grades:
        fig, ax = plt.subplots(figsize=(6, 2))
        ax.text(0.5, 0.5, "No grade data available", ha="center", va="center",
                transform=ax.transAxes)
        ax.axis("off")
        return fig

    grade_labels = {3: "Grade 3", 4: "Grade 4", 5: "Grade 5",
                    6: "Grade 6", 7: "Grade 7", 8: "Grade 8", 11: "Grade 11"}
    n_grades = len(grades)
    n_rows = (n_grades + CHART_COLS - 1) // CHART_COLS
    fig, axes = plt.subplots(n_rows, CHART_COLS,
                              figsize=(CHART_COLS * 4.5, n_rows * 3.2),
                              squeeze=False)

    for idx, grade in enumerate(grades):
        r, c = divmod(idx, CHART_COLS)
        ax = axes[r][c]
        show_legend = idx == 0

        g_state = state_df[state_df["grade"] == grade]
        g_sel = selected_df[selected_df["grade"] == grade] if not is_state_selected else g_state

        state_vals = g_state.groupby("year")["pct_met_and_above"].mean().to_dict()
        sel_vals = g_sel.groupby("year")["pct_met_and_above"].mean().to_dict() if not is_state_selected else state_vals

        _draw_bar_cell(ax, ALL_YEARS, state_vals, sel_vals,
                       grade_labels.get(grade, f"Grade {grade}"), is_state_selected, show_legend)

    for idx in range(n_grades, n_rows * CHART_COLS):
        r, c = divmod(idx, CHART_COLS)
        axes[r][c].axis("off")

    fig.suptitle("Performance by Grade Level (% Met or Exceeded Standard)",
                 fontsize=12, y=1.01)
    fig.tight_layout()
    return fig


def run_analysis(
    entity_type: str,
    entity_label: str,
    test_name: str,
    student_group_id: int,
) -> tuple[plt.Figure, plt.Figure, str]:
    df = get_df()

    test_id = 1 if "English" in test_name or "ELA" in test_name else 2
    df = df[df["test_id"] == test_id]
    df = df[df["student_group_id"] == student_group_id]

    state_df = df[df["type_id"] == TYPE_STATE].copy()

    is_state_selected = entity_type == "State"
    if is_state_selected:
        selected_df = state_df.copy()
    elif not entity_label:
        fig_empty = plt.figure()
        return fig_empty, fig_empty, "Please select an entity."
    else:
        selected_df = select_entity_rows(df, entity_type, entity_label).copy()

    if state_df.empty:
        fig_empty = plt.figure()
        return fig_empty, fig_empty, "No state data found for this selection."

    status = (
        f"State rows: {len(state_df):,} | "
        f"Selected entity rows: {len(selected_df):,} | "
        f"Years: {sorted(state_df['year'].dropna().unique().tolist())}"
    )

    cohort_fig = build_cohort_grid(state_df, selected_df, is_state_selected)
    grade_fig = build_grade_grid(state_df, selected_df, is_state_selected)

    plt.close("all")  # prevent memory leak; figures already returned

    return cohort_fig, grade_fig, status


def build_ui():
    df = get_df()

    district_choices = get_district_choices()
    school_choices = get_school_choices()
    sg_choices = get_student_group_choices()

    tests = (
        df[df["test_id"].isin([1, 2])][["test_id", "test_name"]]
        .drop_duplicates()
        .sort_values("test_id")
    )
    test_choices = tests["test_name"].dropna().tolist()

    with gr.Blocks(title="CAASPP SBAC Score Analysis") as app:
        gr.Markdown("# CAASPP Smarter Balanced Assessment Analysis")
        gr.Markdown(
            "Compare student performance across years, cohorts, and grades. "
            "**Blue** = California statewide · **Red** = Selected entity"
        )

        with gr.Row():
            entity_type_radio = gr.Radio(
                choices=["State", "District", "School"],
                value="State",
                label="Entity",
            )
            entity_dropdown = gr.Dropdown(
                choices=[],
                label="Select District or School",
                visible=False,
                allow_custom_value=False,
                filterable=True,
                scale=3,
            )

        with gr.Row():
            test_radio = gr.Radio(
                choices=test_choices,
                value=test_choices[0] if test_choices else None,
                label="Test",
            )
            sg_dropdown = gr.Dropdown(
                choices=sg_choices,
                value=1,  # All Students
                label="Student Group",
                filterable=True,
                scale=3,
            )

        run_btn = gr.Button("Generate Charts", variant="primary")
        status_text = gr.Textbox(label="Status", interactive=False, lines=1)

        cohort_plot = gr.Plot(label="Performance by Student Cohort")
        grade_plot = gr.Plot(label="Performance by Grade Level")

        def update_entity_dropdown(entity_type):
            if entity_type == "District":
                return gr.update(choices=district_choices, visible=True,
                                 label="Select District", value=None)
            elif entity_type == "School":
                return gr.update(choices=school_choices, visible=True,
                                 label="Select School", value=None)
            else:
                return gr.update(visible=False, value=None)

        entity_type_radio.change(
            update_entity_dropdown,
            inputs=[entity_type_radio],
            outputs=[entity_dropdown],
        )

        def on_run(entity_type, entity_label, test_name, sg_id):
            if not test_name:
                fig_empty = plt.figure()
                return fig_empty, fig_empty, "Please select a test."
            cohort_fig, grade_fig, status = run_analysis(
                entity_type, entity_label or "", test_name, int(sg_id or 1)
            )
            return cohort_fig, grade_fig, status

        run_btn.click(
            on_run,
            inputs=[entity_type_radio, entity_dropdown, test_radio, sg_dropdown],
            outputs=[cohort_plot, grade_plot, status_text],
        )

        app.load(
            lambda: on_run("State", None, test_choices[0] if test_choices else "", 1),
            outputs=[cohort_plot, grade_plot, status_text],
        )

    return app


def main():
    print("Loading data...", flush=True)
    get_df()
    print("Data loaded. Starting Gradio...", flush=True)
    app = build_ui()
    app.launch()


if __name__ == "__main__":
    main()
