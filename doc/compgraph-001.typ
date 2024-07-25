#import "@preview/fletcher:0.5.1" as fletcher: diagram, node, edge

#align(center, text(17pt)[
  *Virtual Tables Comp-Graph*
])

#align(center)[
  v0.1 \
  24.07.2024 \
  Tobias Pietzsch \
]

= Virtual Table Definition

Virtual Table defined by chaining operators, $0$-ary (source tables), unary (most operations are table-in table-out), $n$-ary (appending and concatenating tables).

Operator defined by `TableTransformSpec` ("spec").

The following spec types exist:
`SOURCE`,
`APPEND`,
`CONCATENATE`,
`CONSUMER`,
`MAP`,
`ROWINDEX`,
`SLICE`,
`COLSELECT`,
`ROWFILTER`,
`OBSERVER`.

== Spec Graph

As much as possible stick to the following notation:
#table(
  columns: 3,
  stroke: 0.1pt,
  [*name*],        [*symbol*],             [*node*],
  [`SOURCE`],      [$sigma in Sigma$],     [$S$],
  [`COLSELECT`],   [$chi in Chi$],         [$X$],
  [`ROWFILTER`],   [$phi in Phi$],         [$F$],
  [`SLICE`],       [$lambda in Lambda$],   [$L$],
  [`APPEND`],      [$psi in Psi$],         [$A$],
  [`CONCATENATE`], [$xi in Xi$],           [$C$],
  [`CONSUMER`],    [$omega in Omega$],     [$T$],
  [`MAP`],         [$mu in Mu$],           [$M$],
  [`ROWINDEX`],    [$kappa in Kappa$],     [$K$],
  [`OBSERVER`],    [$omicron in Omicron$], [$O$],
)
For example, `SOURCE` specs will be denoted as $sigma_i$, with $sigma_i in Sigma$ the set of all `SOURCE` specs.
`SOURCE` nodes in the compgraph will be labeled $S_j$.

The set of all possible specs is $Theta = Sigma union Chi union Phi union Lambda union Psi union Xi union Omega union Mu union Kappa union Omicron$.

A virtual table definition (or rather its producing `TableTransform`) translates into a "spec graph":
- Nodes $v in V$ represent `TableTransform`s.
- Edges $e in E$ represent `TableTransform.getPrecedingTransforms()` relationships (pointing from transform to preceeding transform).

For disambigutation, we call the edges _spec edges_.
(Because we will later define different graphs on the same set of nodes).

The _spec graph_ is always a tree.
There is no fork-join allowed: if the same `TableTransform` is re-used in the definition, independent branches are created.
- The tree root is always a `CONSUMER` node.
- All leafs are `SOURCE` nodes.
- Interior nodes have exactly one incoming and exactly one outgoing spec edge,
  except `APPEND` and `CONCATENATE`, which may have more than one incoming spec edge.

A node $v = (i_v, theta_v)$ has a unique id $i_v in NN$ and a spec $theta_v in Theta$.\
Note that the multiple nodes can have the same spec (because of fork-join unrolling, or simply because the same spec occurs in different contexts).

An edge $e = (v,v') in E subset V times V$ means $v'$ is a preceeding transform of $v$.


== Spec details

*`SOURCE`:*
 $sigma in Sigma$ is a tuple $sigma = (t_sigma, n_sigma)$, where
$t_sigma$ is the UUID of a source table, and
$n_sigma$ is the number of columns in the source table.



== Number of columns

The number of columns of (the virtual table corresponding to the sub-tree rooted in) a node $v$ is

#let ncols = $op("ncols")$
$
ncols(v) := cases(
  n_sigma   & "if" theta_v = sigma in Sigma,
  "TODO"    & "if" theta_v = mu in Mu "MAP",
  "TODO"    & "if" theta_v = kappa in Kappa "ROWINDEX",
  "TODO"    & "if" theta_v = chi in Chi "COLSELECT",
  "TODO"    & "if" theta_v = psi in Psi "APPEND",
  "TODO"    & "if" theta_v = xi in Xi "CONCATENATE",
  ncols(v') & "otherwise, with" (v,v') in E,
) $












#pagebreak()

= draft

- `SOURCE`, `APPEND`, `CONCATENATE`, `CONSUMER` \
  These form the 

- `MAP`, `ROWINDEX`, `SLICE`, `COLSELECT`, `ROWFILTER`, `OBSERVER`


#diagram(
  // debug: true, // show a coordinate grid
  spacing: (15pt, 15pt), // small column gaps, large row spacing
  node-stroke: 0.5pt,
  node-fill: gray.lighten(80%),
  node-shape: rect,
  // node-inset: 7pt,
  node((0,0), `Source`),
  node((1,0), `Source`),
  node((0,1), `Slice`),
  node((1,1), `Slice`),
  node((0.5,2), `Append`),
  node((0.5,3), `Consumer`, extrude: (0, 3)),
  edge((0,0), (0,1), "->"),
  edge((1,0), (1,1), "->"),
  edge((1,1), (0.5,2), "->"),
  edge((0,1), (0.5,2), "->"),
  edge((0.5,2), (0.5,3), "->"),
)
