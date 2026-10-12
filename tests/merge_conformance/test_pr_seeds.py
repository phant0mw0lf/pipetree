"""The fixed PR seed lists reach every coverage name the nightly range reaches (no Spark)."""

from __future__ import annotations

import pytest

from pipetree.testing.merge_gen import STRATEGIES, coverage, generate_case
from tests.merge_conformance.pr_seeds import PR_SEEDS, RANGE, TARGET, derive, reachable

REGEN = "uv run python -m tests.merge_conformance.pr_seeds --write"


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_pr_seeds_reach_every_reachable_coverage_name(strategy):
    got: set[str] = set()
    for seed in PR_SEEDS[strategy]:
        got |= coverage(generate_case(seed, strategy))
    missing = reachable(strategy) - got
    assert not missing, (
        f"the fixed PR seeds of {strategy} no longer reach {sorted(missing)} (the generator "
        f"changed); regenerate them with `{REGEN}`"
    )


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_pr_seeds_are_valid_and_about_the_default_count(strategy):
    seeds = PR_SEEDS[strategy]
    assert seeds == sorted(set(seeds)), f"unsorted or duplicate seeds; run `{REGEN}`"
    assert all(0 <= s < RANGE for s in seeds), f"seed outside 0..{RANGE - 1}; run `{REGEN}`"
    assert TARGET <= len(seeds) <= TARGET + 5, f"unexpected number of seeds; run `{REGEN}`"


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_pr_seeds_are_what_the_script_derives(strategy):
    assert PR_SEEDS[strategy] == derive(strategy), f"stale PR seeds; run `{REGEN}`"
