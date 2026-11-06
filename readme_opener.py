"""Replace the README's intro with a summary aimed at a thirty-second skim."""
import pathlib
import sys

p = pathlib.Path("README.md")
if not p.exists():
    sys.exit("run this from ~/str-pipeline")
s = p.read_text()

if "> **Status:**" not in s or not s.lstrip().startswith("#"):
    sys.exit("README does not start the way this expects; send Claude the first 12 lines")

h1_end = s.index("\n", s.index("# ")) + 1
status_at = s.index("> **Status:**")

INTRO = """
An Airflow and dbt pipeline that turns Inside Airbnb snapshots into the three
numbers hotel revenue management runs on, occupancy, ADR and RevPAR, and adds a
backtested weekly demand forecast on top of them.

**146,490 listing-months modelled · 24 tests on every build · demand forecast at
7.83% MAPE twelve weeks out, 34% better than a seasonal baseline**

The pipeline's more interesting output was what it found in its own inputs. Two
of the four source snapshots are defective in ways a row-count check passes
without complaint: one contains every listing and no prices at all, and another
is missing 2,533 listings that a listing-ID trace identifies as missed by the
scrape rather than delisted from the market. Both are now caught automatically.
[The findings are below](#results), along with the two modelling approaches the
backtest ruled out.

"""

s = s[:h1_end] + INTRO + s[status_at:]
p.write_text(s)

head = s.splitlines()[:22]
print("\n".join(head))
print("\n---\nREADME is now", len(s.splitlines()), "lines")
