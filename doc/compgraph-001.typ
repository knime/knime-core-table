#import "@preview/fletcher:0.5.1" as fletcher: diagram, node, edge

#align(center, text(17pt)[
  *Virtual Tables Comp-Graph*
])

#align(center)[
  v0.1 \
  26.07.2024 \
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



== Predecessors of a node

Let $Pi(v) = {v' | (v,v') in E }$ be the set of spec predecessors of $v$.

Most nodes have exactly 1 predecessor, except `SOURCE`s which have 0, and `APPEND`s and `CONCATENATE`s which usually have more than 1.

$
|Pi(v)| cases(
  =  0 & "if" theta_v in Sigma,
  >= 1 & "if" theta_v in Psi and Xi,
  =  1 & "otherwise"
)
$

If $|Pi(v)|=1$, we may use $pi(v) in Pi(v)$ denotes the single predecessor.

== Number of columns

The number of columns of (the virtual table corresponding to the sub-tree rooted in) a node $v$ is

#let ncols = $op("ncols")$
$
ncols(v) := cases(
  n_sigma      & "if" theta_v = sigma in Sigma,
  "TODO"       & "if" theta_v = mu in Mu "MAP",
  "TODO"       & "if" theta_v = kappa in Kappa "ROWINDEX",
  "TODO"       & "if" theta_v = chi in Chi "COLSELECT",
  "TODO"       & "if" theta_v = psi in Psi "APPEND",
  "TODO"       & "if" theta_v = xi in Xi "CONCATENATE",
  ncols(pi(v)) & "otherwise",
) $


== Inputs/outputs of a node

Every node has _inputs_ and _outputs_ that represent the columns "flowing into and out of" the node.

If node $v$ is not a `SOURCE`, `APPEND`, or `CONCATENATE`, then $|Pi(v)|=1$, and:\
The node $v$ has $k_v$ inputs labeled $alpha_v^i$ with $1<=i<=k_v=ncols(pi(v))$.\
#text(red)[TODO: or should $i$ rather be zero-based?\
The node $v$ has $k_v$ inputs labeled $alpha_v^i$ with $0<=i<k_v=ncols(pi(v))$.]

`SOURCE` nodes have no inputs.

`APPEND` #text(red)[TODO]
\
\
\
\
\

`CONCATENATE` #text(red)[TODO]
\
\
\
\
\



 #text(red)[TODO: Outputs]\
 $beta_v^i$


== Accesses, access tracing

In practice, every value flowing through the virtual table is a `ReadAccess`
that is _produced_ (created and written) somewhere (e.g. a `SOURCE`) and _required_ (read) elsewhere (e.g. a `ROWFILTER`).

Each node _input_ or _output_ refers to such an _access_.

There is an implied equivalence relation $eq.triple$ between access references:
For example, _accesses_ are often just passed through a node. A `ROWFILTER` node $v$ directly passes its inputs as outputs to the next node, that is $beta_v^i eq.triple alpha_v^i$.
Similarly, the inputs of node $v$ are the outputs of its spec predecessor $v'=pi(v)$,
that is $alpha_v^i eq.triple beta_v'^i$.

Every equivalence class in $eq.triple$ contains exactly one producer, that is, the node output where the `ReadAccess` is created and written to.

#let union = $op("union")$
#let find = $op("find")$
The process of programmatically establishing $eq.triple$ is _access tracing_.
This is implemented as a *union-find* algorithm.
We implement *union* to link access reference sets with representatives $alpha, beta$ such that the set leader ($find(alpha)$ or $find(beta)$) is chosen as the "more predecessor one", i.e., the one occuring earlier in the virtual table definition.
This will ultimately lead to the producer being the set leader.

(Note that accesses do not "survive" `APPEND` or `CONCATENATE`. Therefore, $union$ only happens within a totally ordered sequence of nodes, and consequently "the more predecessor one" is always well-defined.)

The following equivalences hold:
 #text(red)[TODO]



== Required inputs

Node _uses_ some inputs to produce its outputs.

_Used inputs_ come out of the spec.
Note that passed-through accesses are not (necessarily) _used_.

Recursively define property "_required_" on nodes and accesses:
- `CONSUMER` is required.
- All accessed _used_ by a required node are required.
- Node is required if it produces at least one access that is required.

Special case:
`APPEND` and `CONCATENATE` require inputs corresponding to required outputs.

Implied partial order $F$ on nodes:
$(v,u) in F$ if $v$ uses an input that is produced by $v$.\
$find(alpha_v^i) = beta_u^j$

== Artifical accesses

$F$ will be used to determine in which order nodes are executed.

Takes care of data dependencies. For example, if a `MAP` uses inputs of a 


#pagebreak()

= Optimizations

#text(red)[*TODO:*]
Think through these optimization steps.\
How do they work by just relinking access usage?\
Is there anything in input/output linking etc that would prevent them?\
Are input/output still meaningful after applying them?\
Can we still recover a spec graph after applying them?\

- eliminateRowIndexes
- eliminateAppends
- mergeSlices
- mergeRowIndexSiblings
- mergeRowIndexSequence
- moveSlicesBeforeObserves
- moveSlicesBeforeAppends
- moveSlicesBeforeRowIndexes
- moveSlicesBeforeConcatenates
- eliminateSingletonConcatenates

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
