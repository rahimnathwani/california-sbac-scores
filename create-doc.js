const { Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType,
        LevelFormat, Table, TableRow, TableCell, BorderStyle, WidthType,
        ShadingType, VerticalAlign, PageNumber, Header, Footer } = require('/Users/rahim/.volta/tools/image/node/24.13.0/lib/node_modules/docx');
const fs = require('fs');

const tableBorder = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const cellBorders = { top: tableBorder, bottom: tableBorder, left: tableBorder, right: tableBorder };

function h(text, level) {
  return new Paragraph({ heading: level, children: [new TextRun(text)] });
}

function p(runs) {
  if (typeof runs === 'string') runs = [new TextRun(runs)];
  return new Paragraph({ children: runs });
}

function bullet(text, ref = "bullets") {
  return new Paragraph({ numbering: { reference: ref, level: 0 }, children: [new TextRun(text)] });
}

function headerCell(text, fill = "D5E8F0") {
  return new TableCell({
    borders: cellBorders,
    shading: { fill, type: ShadingType.CLEAR },
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ text, bold: true, size: 20 })] })]
  });
}

function dataCell(text, width = 3120) {
  return new TableCell({
    borders: cellBorders,
    width: { size: width, type: WidthType.DXA },
    children: [new Paragraph({ children: [new TextRun({ text, size: 20 })] })]
  });
}

const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 24 } } },
    paragraphStyles: [
      { id: "Title", name: "Title", basedOn: "Normal",
        run: { size: 52, bold: true, color: "1A3A5C", font: "Arial" },
        paragraph: { spacing: { before: 0, after: 160 }, alignment: AlignmentType.LEFT } },
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, color: "1A3A5C", font: "Arial" },
        paragraph: { spacing: { before: 360, after: 120 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, color: "2E6DA4", font: "Arial" },
        paragraph: { spacing: { before: 240, after: 80 }, outlineLevel: 1 } },
      { id: "Callout", name: "Callout", basedOn: "Normal",
        run: { size: 22, italics: true, color: "444444", font: "Arial" },
        paragraph: { spacing: { before: 120, after: 120 },
          indent: { left: 720, right: 720 } } },
    ]
  },
  numbering: {
    config: [
      { reference: "bullets",
        levels: [{ level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 }, spacing: { after: 80 } } } }] },
      { reference: "bullets2",
        levels: [{ level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 }, spacing: { after: 80 } } } }] },
      { reference: "bullets3",
        levels: [{ level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 }, spacing: { after: 80 } } } }] },
      { reference: "bullets4",
        levels: [{ level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 }, spacing: { after: 80 } } } }] },
      { reference: "bullets5",
        levels: [{ level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 }, spacing: { after: 80 } } } }] },
    ]
  },
  sections: [{
    properties: {
      page: { margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 } }
    },
    footers: {
      default: new Footer({ children: [new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [new TextRun({ text: "Page ", size: 18, color: "888888" }),
                   new TextRun({ children: [PageNumber.CURRENT], size: 18, color: "888888" }),
                   new TextRun({ text: " of ", size: 18, color: "888888" }),
                   new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18, color: "888888" })]
      })] })
    },
    children: [
      // Title block
      new Paragraph({ heading: HeadingLevel.TITLE, children: [new TextRun("California SBAC Test Scores")] }),
      p([new TextRun({ text: "A guide to the dataset: what we have, how it was assembled, and what you can do with it", italics: true, color: "555555", size: 22 })]),
      new Paragraph({ spacing: { after: 320 }, children: [new TextRun({ text: "March 2026", size: 20, color: "888888" })] }),

      // ── Section 1 ──────────────────────────────────────────
      h("What Is This Data?", HeadingLevel.HEADING_1),
      p("Every spring, California public school students in grades 3 through 8 and grade 11 take standardized tests in English Language Arts (ELA) and Math. These are called the Smarter Balanced Assessments (SBAC), and they are administered statewide as part of the California Assessment of Student Performance and Progress (CAASPP) program."),
      new Paragraph({ spacing: { after: 120 }, children: [] }),
      p("The state releases the results publicly every year, broken down by:"),
      bullet("School, district, county, and state level", "bullets"),
      bullet("Subject: ELA and Math", "bullets"),
      bullet("Grade level (3, 4, 5, 6, 7, 8, and 11)", "bullets"),
      bullet("Student group: All Students, by race/ethnicity, by economic disadvantage, by English learner status, by disability status, and more", "bullets"),
      new Paragraph({ spacing: { after: 120 }, children: [] }),
      p("The key metric we focus on is the percentage of students who scored at or above the \u201CMet Standard\u201D level \u2014 roughly, the share of students considered on track for college and career readiness."),
      new Paragraph({ spacing: { after: 120 }, children: [] }),
      p([new TextRun({ text: "Years covered: ", bold: true }), new TextRun("2015 through 2025, with no 2020 data (testing was cancelled due to COVID-19).")]),

      // ── Section 2 ──────────────────────────────────────────
      h("Why Processing Was Needed", HeadingLevel.HEADING_1),
      p("The state does not publish one tidy spreadsheet. Instead, it releases a separate ZIP file for each year \u2014 ten files in total \u2014 and the format of those files changed significantly over the decade. Before you can do any analysis, someone has to figure out how to read each year\u2019s files, reconcile the differences, and stack everything into a single table."),

      h("The raw files came in two formats", HeadingLevel.HEADING_2),

      // Format comparison table
      new Table({
        columnWidths: [2200, 3580, 3580],
        margins: { top: 80, bottom: 80, left: 140, right: 140 },
        rows: [
          new TableRow({
            tableHeader: true,
            children: [
              headerCell(""),
              headerCell("2015\u20132019"),
              headerCell("2021\u20132025"),
            ]
          }),
          new TableRow({ children: [
            dataCell("File delimiter", 2200),
            dataCell("Comma (CSV)", 3580),
            dataCell("Caret (^) separated", 3580),
          ]}),
          new TableRow({ children: [
            dataCell("Column names", 2200),
            dataCell("\"Subgroup ID\", \"Test Id\", \"Students Tested\"", 3580),
            dataCell("\"Student Group ID\", \"Test ID\", \"Total Students Tested\" (2024\u20132025)", 3580),
          ]}),
          new TableRow({ children: [
            dataCell("School/district names", 2200),
            dataCell("Not included \u2014 must join from a separate entities lookup file", 3580),
            dataCell("2021\u20132023: same as earlier years. 2024\u20132025: names included inline.", 3580),
          ]}),
        ]
      }),
      new Paragraph({ spacing: { after: 160 }, children: [] }),

      h("Other complications", HeadingLevel.HEADING_2),
      bullet("Each year\u2019s ZIP also contains lookup tables (student group labels, test names, school identifiers) that need to be joined in to make the data readable.", "bullets2"),
      bullet("The main data files contain rows for every possible combination of school, grade, subject, and student group \u2014 including aggregates (e.g., \u201CAll Grades\u201D) that need to be filtered out to avoid double-counting.", "bullets2"),
      bullet("Score columns are sometimes blank when a cell\u2019s count is too small to report (privacy suppression). These need to be handled carefully so they don\u2019t get treated as zeros.", "bullets2"),

      h("What the ingestion script does", HeadingLevel.HEADING_2),
      p("A Python script (1-ingest-original-files.py) reads all ten ZIP files, normalizes the column names and delimiters, joins in the lookup tables, filters out aggregate grade rows, and writes everything into a single Parquet file. Parquet is a compressed columnar format \u2014 think of it as a very efficient version of a CSV that loads much faster."),
      new Paragraph({ spacing: { after: 120 }, children: [] }),
      p([new TextRun({ text: "The result: ", bold: true }), new TextRun("a single 649 MB file with roughly 27 million rows, one per unique combination of year, location, grade, subject, and student group.")]),

      // ── Section 3 ──────────────────────────────────────────
      h("What the Data Looks Like", HeadingLevel.HEADING_1),
      p("Each row in the final dataset represents one \u201Ccell\u201D: a specific school (or district, county, or state), in a specific year, for a specific grade, subject, and student group. The key columns are:"),
      new Paragraph({ spacing: { after: 120 }, children: [] }),

      new Table({
        columnWidths: [2800, 6560],
        margins: { top: 80, bottom: 80, left: 140, right: 140 },
        rows: [
          new TableRow({ tableHeader: true, children: [
            headerCell("Column"),
            headerCell("What it means"),
          ]}),
          new TableRow({ children: [dataCell("year", 2800), dataCell("School year (e.g., 2023 = the 2022\u201323 school year)", 6560)] }),
          new TableRow({ children: [dataCell("type_id", 2800), dataCell("Level: 4=State, 5=County, 6=District, 7=School", 6560)] }),
          new TableRow({ children: [dataCell("county_name, district_name, school_name", 2800), dataCell("Name of the entity", 6560)] }),
          new TableRow({ children: [dataCell("grade", 2800), dataCell("3, 4, 5, 6, 7, 8, or 11", 6560)] }),
          new TableRow({ children: [dataCell("test_id / test_name", 2800), dataCell("1 = SB ELA, 2 = SB Math", 6560)] }),
          new TableRow({ children: [dataCell("student_group_id / student_group_name", 2800), dataCell("E.g., \u201CAll Students\u201D, \u201CHispanic or Latino\u201D, \u201CEconomically Disadvantaged\u201D", 6560)] }),
          new TableRow({ children: [dataCell("pct_met_and_above", 2800), dataCell("% of tested students who met or exceeded standard (the main outcome metric)", 6560)] }),
          new TableRow({ children: [dataCell("students_tested", 2800), dataCell("Number of students who took the test in that cell", 6560)] }),
          new TableRow({ children: [dataCell("cohort_year", 2800), dataCell("Derived field: the year that cohort was in 3rd grade (year \u2212 grade + 3). Lets you follow the same students across years.", 6560)] }),
        ]
      }),
      new Paragraph({ spacing: { after: 160 }, children: [] }),

      // ── Section 4 ──────────────────────────────────────────
      h("What You Can Do With It Now", HeadingLevel.HEADING_1),
      p("With everything in one place, a wide range of questions become straightforward to answer. Here are some examples organized by type of analysis:"),

      h("Trend analysis over time", HeadingLevel.HEADING_2),
      bullet("How has the percentage of students meeting ELA standards changed in your district from 2015 to 2025?", "bullets3"),
      bullet("Which districts showed the steepest decline after COVID, and which recovered fastest?", "bullets3"),
      bullet("Did the ELA/Math gap widen or narrow over time in your county?", "bullets3"),

      h("Equity analysis by student group", HeadingLevel.HEADING_2),
      bullet("What is the proficiency gap between economically disadvantaged and non-disadvantaged students at each grade level?", "bullets4"),
      bullet("How do English Learners\u2019 scores compare to the state average, and has that gap changed?", "bullets4"),
      bullet("Which schools have the smallest gaps between student groups \u2014 and what might they be doing differently?", "bullets4"),

      h("Cohort tracking", HeadingLevel.HEADING_2),
      p("The cohort_year field lets you follow the same group of students as they move through school. For example, students who were in 3rd grade in 2015 were in 6th grade in 2018 and in 11th grade in 2023. You can ask:"),
      bullet("Did scores for a particular cohort improve or decline as they moved from elementary to middle to high school?", "bullets5"),
      bullet("Which cohorts were most affected by the pandemic (missing 2020 data)?", "bullets5"),

      h("School and district benchmarking", HeadingLevel.HEADING_2),
      p("Because the data includes all schools in California, you can compare any school or district against:"),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, children: [new TextRun("The state average")] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, children: [new TextRun("Similar schools (similar size, demographics, or location)")] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, children: [new TextRun("Its own historical performance")] }),

      new Paragraph({ spacing: { after: 240 }, children: [] }),

      // Callout box (styled paragraph)
      new Paragraph({
        style: "Callout",
        border: {
          left: { style: BorderStyle.SINGLE, size: 12, color: "2E6DA4", space: 12 }
        },
        children: [
          new TextRun({ text: "A note on the numbers: ", bold: true, italics: true, color: "1A3A5C" }),
          new TextRun({ text: "Proficiency rates are a useful signal, but they reflect many factors beyond school quality \u2014 including student demographics, community resources, and test-taking conditions. When comparing schools or districts, always consider the student group composition. The student group breakdowns in this dataset are what make it possible to see past aggregate numbers and understand who is being served well and who isn\u2019t.", italics: true, color: "444444" })
        ]
      }),

      new Paragraph({ spacing: { after: 240 }, children: [] }),

      // ── Section 5 ──────────────────────────────────────────
      h("Technical Notes for Spreadsheet Users", HeadingLevel.HEADING_1),
      p("If you want to work with this data outside the analysis app, here are a few things to know:"),
      new Paragraph({ spacing: { after: 80 }, children: [] }),
      new Paragraph({ numbering: { reference: "bullets2", level: 0 }, children: [
        new TextRun({ text: "File format: ", bold: true }),
        new TextRun("The data is stored as a Parquet file (sbac_data.parquet). Excel cannot open this directly, but Python (pandas/polars) or DuckDB can read it instantly and export to CSV for use in Excel.")
      ]}),
      new Paragraph({ numbering: { reference: "bullets2", level: 0 }, children: [
        new TextRun({ text: "Size: ", bold: true }),
        new TextRun("27 million rows. A CSV version would be several gigabytes and would crash Excel. For spreadsheet work, always filter to a subset first (e.g., one district, one subject, one student group).")
      ]}),
      new Paragraph({ numbering: { reference: "bullets2", level: 0 }, children: [
        new TextRun({ text: "Blank proficiency values: ", bold: true }),
        new TextRun("Some cells have no pct_met_and_above value \u2014 this means the state suppressed the score because the group was too small (typically fewer than 11 students). Treat these as missing data, not zeros.")
      ]}),
      new Paragraph({ numbering: { reference: "bullets2", level: 0 }, children: [
        new TextRun({ text: "Filtering to schools only: ", bold: true }),
        new TextRun("Use type_id = 7 to get school-level rows. County (5), district (6), and state (4) aggregates are also included and useful for benchmarking but should not be mixed into school-level calculations.")
      ]}),
      new Paragraph({ numbering: { reference: "bullets2", level: 0 }, children: [
        new TextRun({ text: "\u201CAll Students\u201D group: ", bold: true }),
        new TextRun("Use student_group_id = 1 to get the overall proficiency rate. All other student group rows are subsets of this number.")
      ]}),

      new Paragraph({ spacing: { after: 240 }, children: [] }),
    ]
  }]
});

Packer.toBuffer(doc).then(buffer => {
  fs.writeFileSync("sbac-data-guide.docx", buffer);
  console.log("Created sbac-data-guide.docx");
});
