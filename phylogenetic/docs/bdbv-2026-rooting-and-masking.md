# Rooting and masking the 2026 BDBV outbreak build

Scope: `bdbv/drc-uganda-2026`. Genome length 18940 nt, 1104 sequences in the
subsample at the time of writing.

## Rooting

The build is rooted on the old-BDBV outgroup (`reroot_tree`, pruned afterwards)
and `refine --keep-root` preserves that root.

Clock rooting is not usable here. The outbreak spans 5.5 months, over which about
9.7 substitutions accumulate, against a root-to-tip residual of ±2.6
substitutions (R²=0.43, 1.1×10⁻³ subs/site/yr). The basal splits are 1–2
substitutions deep, so `--root best` is choosing between candidates separated by
less than the noise: six independent analyses of the same data returned six
different basal lineages, and the root-placement bipartition carries 21% UFboot.

The clade the clock root made basal in the deployed build — 45 sequences defined
by `A3092G` and `A8915C` — is a genuine clade (85.2/94) but sits 8–18 nodes deep
under outgroup rooting, and is the lineage *furthest* from the old-BDBV outgroup
of all basal candidates.

| root | TMRCA |
|---|---|
| outgroup-fixed | 2026.308 |
| clock-chosen | 2026.299 |
| deployed build | 2026.189 (CI 2026.143–2026.279) |

The base of the outbreak is close to a star. No basal lineage carries the root
signal: of the 100 tips closest to the old-BDBV outgroup consensus, 90 fall in
the largest basal lineage against 78 expected from its size alone. `--polytomy`
is therefore passed to the tree builder, so zero-length branches are left
unresolved rather than given an arbitrary resolution.

## Masking

### Termini

Coverage falls off a cliff at both ends. 537 positions — 1–65 and 18469–18940 —
are gap in the majority of sequences, and 1066 of 1104 sequences stop short of
18891.

| position | % gap | % N | % ACGT |
|---|---|---|---|
| 1 | 100.0 | 0.0 | 0.0 |
| 50 | 92.9 | 0.0 | 7.1 |
| 100 | 5.6 | 0.2 | 94.2 |
| 18400 | 0.4 | 6.5 | 93.1 |
| 18500 | 71.9 | 0.6 | 27.4 |
| 18800 | 72.2 | 0.0 | 27.8 |
| 18900 | 97.1 | 0.0 | 2.9 |

These unsequenced ends are written as `-`, not `N`. TreeTime reads `N` as
missing but `-` as a deletion — a real character state. The mask therefore has
to reach the cliff: at `beginning: 50` / `end: 50` only 100 of the 537 positions
became `N`, the other 437 stayed `-`, and the majority state won at the root.
The resulting root sequence held exactly those 437 gaps, spanning 51..18890, and
94,505 gap-involving mutations were painted onto the branches of every lineage
that *did* cover the ends — against 51 in a tree whose root happened to fall
inside the well-covered bulk.

Masking at `beginning: 65` / `end: 472` turns all 537 into `N`.

Left unmasked, these positions also carry signal that is really sequencing
extent rather than evolution: sequences whose runs stopped at the same
coordinate group together.

### Hard masking

`augur ancestral` read the unmasked alignment, so masked positions reappeared as
branch mutations on a tree that was built without them. In the deployed tree a
basal 3-tip branch carries 12 such mutations between 18905 and 18926 — inside
the terminal mask window — alongside its one real substitution, `T16608C`.

`mask: hard: true` points ancestral at a masked alignment instead
(`rule mask_without_outgroup`, which masks the subsample without the outgroup so
the sequences match the tree's tips once the outgroup is pruned).
`--keep-ambiguous`, already set for this build, stops ancestral inferring the
masked positions back a second way.

| | 50/50 mask, clock root | cliff mask, hard, outgroup root |
|---|---|---|
| tips | 1100 | 1093 |
| total nuc mutations | 1,087,954 | 608,489 |
| gap-involving mutations | 51 | 21,720 |
| gaps in root sequence | 0 | 0 |
| basal children | 11 | 27 |
| root date | 2026.189 | 2026.229 |

The remaining 21,720 gap-involving mutations are tip-level: the root is clean, so
they are deletions and internal missing data on individual branches rather than
a gap state inherited from the root.

### Recurrent sites

Counted as independent events on the deployed tree: 1608 substitutions over 1236
sites, of which 54 sites take ≥3 events and 16 take ≥4. 60.4% of all events are
`T→C` or `A→G`, the ADAR pair.

A high event count alone does not make a site noise. What distinguishes noise is
whether the events are scattered:

| site | events | tips carrying it | largest single event | share |
|---|---|---|---|---|
| 5246 | 63 | 256 | 37 | 14% |
| 5251 | 17 | 23 | 5 | 22% |
| 3287 | 6 | 15 | 9 | 60% |
| 10319 | 29 | 1065 | 863 | 81% |
| 8915 | 6 | 52 | 45 | 87% |
| 268 | 6 | 138 | 130 | 94% |

Only 5246 is masked. Its events are spread across the whole tree, the largest
covering 37 of the 256 tips that carry it, and it reverts (`A5246G` 55,
`G5246A` 8).

268 is the counterexample: 6 events, but 130 of its 138 tips sit in a single
clade. It is a real marker the tree search left slightly ragged, and masking it
would discard signal. 10319 behaves similarly at 81%, though its 28 reversion
events are worth revisiting.

## Other settings

`min_length: 15152` — 80% of 18940. The `nextclade_qc=bad` exclusion already
floors coverage near there, so this drops few sequences; it makes the floor
explicit rather than incidental.

Tree args are augur's defaults (`--ninit 2 -n 2 --epsilon 0.05 -T AUTO --redo`)
plus `--polytomy`. The previous `--ninit 100 --epsilon 0.01` cost about 20
minutes on 1100 sequences against about 4 for the defaults.

## Open

- 10319 defines the largest basal split and takes 29 events, 26 of them
  reversions. Whether the reversions are real or search artifacts is unresolved.
- The ragged edges on 268 and 10319 suggest the tree search is under-resolving
  low-divergence structure. IQ-TREE 3's `--pathogen` mode (CMAPLE) targets
  exactly this and has not been tried.
- `augur mask` has no gap-to-N option, so the cliff mask is a blunt instrument:
  it discards the ends for the minority of sequences that do cover them.
