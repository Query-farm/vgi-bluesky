# VGI worker notebook standard

This is the working standard established by the three `vgi-perps` examples. Apply
it when creating notebooks for other workers; adapt the analytical question and
source handling to the worker rather than copying perps-specific assumptions.

## Reader experience

1. Put the official **Query.Farm Strata Sun + wordmark** at both the top and bottom
   of every notebook, linked to <https://query.farm/>. Use the unchanged adaptive
   SVG from the website media kit (`logo/wordmark-adaptive.svg`), stored locally in
   `notebooks/assets/`. Place the footer after cleanup so it closes the notebook.
   Keep the VGI logo near the top, linked to <https://query.farm/vgi/>, with separate
   Query.Farm and VGI links. Preserve both marks' colors and proportions.
2. Open with a concrete question and a short explanation of the investigation.
   Give each notebook one coherent story; split substantially different questions
   into separate, independently runnable notebooks.
3. Show a small **parameters** cell before setup. Use named values with units,
   sensible defaults, accepted ranges and date semantics. Explain how to restart
   and run all after editing. Fetch and filter windows may differ in replay mode.
4. Collapse imports, presentation helpers, plotting source and cleanup. Keep
   parameter cells, **VGI connection SQL (`INSTALL`, `LOAD`, `ATTACH`)**, analytical
   SQL, useful diagnostics, tables, charts and interpretations visible.
5. Introduce each query with what it will establish. After its result, explain
   what the evidence supports and the next question it raises. Derive numerical
   claims and rankings from the returned data instead of hardcoding the saved run.
6. End with one focused experiment: change one parameter, make a prediction,
   identify the output to compare, explain the limit of that comparison, and give
   the original value needed to reset it.

## SQL and execution

- Store examples under `notebooks/`, with numbered, descriptive `.ipynb` names and
  a README containing a notebook index and exact environment/kernel instructions.
- Use Haybarn with the real VGI extension and worker. Prefer JupySQL `%%sql` cells
  for discovery, retrieval, filtering, joins, aggregation and domain calculations.
  Python handles configuration, visualization and result-derived prose.
- Make the connection mechanism visible as a literal ATTACH LOCATION. Use a tested published release (prefer pinned github-auto), pinned package runner, or remotely available Git revision. Do not require a local worker build or interpolate a hidden Python worker path. Document runtime prerequisites and upgrading the pin. Notebook helpers/assets may remain local companions; notebooks must run from their own directory without the worker checkout.
- Use a separate locked notebook environment when visualization dependencies or
  engine versions differ from the worker runtime. Preserve compatible SDK/engine
  versions. Do not require a globally installed Jupyter kernel.
- Make every notebook run from a fresh kernel without another notebook's state.
  Materialize data that will be reused, so drawing a chart does not repeat API calls.
  With Haybarn/JupySQL, use ordinary in-memory tables: temporary tables can be
  cursor-local. Close the connection at the end.
- Save executed outputs after successful execution. A failed run must not replace
  the saved successful example. Use parameter overrides and separate output copies
  when validating experiments or live mode.
- Add a complementary worker when it answers a specific part of the question.
  Show its connection and any join keys, units and timing assumptions.

## Data modes and interpretation

- If replay is available, default to a reproducible recorded example. Prominently
  distinguish recorded responses, synthetic inputs, model assumptions and live data.
  A replayed request still needs to call the real worker through VGI.
- Expose `DATA_MODE` when supporting both replay and live. Switching to live must
  select appropriate recent default windows and update labels, checks and prose.
  Disable inherited recording or replay settings that could silently change modes.
- Validate parameters before requesting data. Document recorded assets and windows;
  reject unsupported replay requests with an actionable message. Filter within
  recorded coverage where useful. Never silently substitute fixtures for live data.
- Specify UTC and inclusive/exclusive endpoints. Capture a run time once where
  consistency matters. Distinguish that clock from actual source timestamps;
  independent responses are not an atomic snapshot.
- Show availability diagnostics. Keep missing, failed, empty and zero-valued results
  distinct. Do not rank an unavailable measurement as zero. State required-source
  failures clearly; label omitted optional charts and unsupported calculations.
- Validate relationships that remain true across reasonable parameter choices:
  unit conversions, unique identifiers, bounds and agreement with a worker result.
  Avoid assertions that demand one fixture's counts, ranking or market direction.
- Keep model assumptions and formula applicability next to their results. When a
  source rule changes, omit an inapplicable explanation explicitly or implement the
  new rule; do not silently display an incorrect curve.

## Visual language

- Use Seaborn for a consistent theme and Matplotlib for precise layout and labels.
  Choose an interactive library only when interaction helps the specific question.
- Give charts a readable title, units, time context and source/provenance footer.
  Keep colors consistent for the same entities and reinforce them with labels,
  legends, markers or line styles. Use sign-aware colors for signed quantities.
- Let limits and annotations follow the selected data. Share scales for comparisons
  when appropriate; disclose independent or logarithmic scales. Do not substitute
  zero for unavailable values. Separate quantities with different units into panels.
- Use result-derived callouts for salient values; explain their significance in
  visible prose. Review the rendered plots for clipped labels and overlapping text.
- Save embedded chart outputs so the notebook is useful before the reader runs it.

## Cell conventions and shared helpers

| Cell role | Source | Output | Tags |
|---|---|---|---|
| Parameters | Visible | Visible | `parameters` |
| Python environment/theme | Collapsed | Collapsed | `setup`, `hide-input`, `hide-output` |
| VGI connection and analytical SQL | Visible | Visible | Optional descriptive tag |
| Plot implementation | Collapsed | Visible | `hide-input` |
| Result-derived interpretation | Collapsed | Visible | `interpretation`, `hide-input` |
| Cleanup | Collapsed | Collapsed | `cleanup`, `hide-input`, `hide-output` |

Set `metadata.jupyter.source_hidden` and `metadata.jupyter.outputs_hidden` explicitly;
set `metadata.collapsed` for collapsed output. Tags supplement those settings.
VS Code can keep folding in editor state independently of notebook metadata;
check the actual editor preview. SQL connection cells must remain expanded.

Keep shared helpers small and inspectable: formatting, provenance, parameter
validation, worker command construction and chart presentation. Keep analytical
SQL in the notebooks. In this repository, see `notebook_support.py`; its fixture
windows and subprocess environment are specific to `vgi-perps`.

Before delivery, execute defaults, execute the proposed one-parameter experiments,
check supported live paths, inspect saved outputs and metadata, and run required
repository checks. Record any external source failures accurately. Automation of
these checks can be added separately; this standard does not install a CI workflow.
