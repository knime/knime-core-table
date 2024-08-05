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
  >= 1 & "if" theta_v in Psi union Xi,
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


== In/out columns of a node

Every node has _in-cols_ and _out-cols_ that represent the columns "flowing into and out of" the node.

Node $v$ has $ncols(v)$ _out-cols_ labeled $beta_v^i$ with $0<=i<ncols(v)$.

If node $v$ is not a `SOURCE`, `APPEND`, or `CONCATENATE`, then $|Pi(v)|=1$, and:\
The node $v$ has $ncols(pi(v))$ _in-cols_ labeled $alpha_v^i$ with $0<=i<ncols(pi(v))$.

`SOURCE` nodes have no inputs.

`APPEND` #text(red)[TODO]

`CONCATENATE` #text(red)[TODO]



== Inputs/outputs of a node

Often, columns just "flow through" a node.
For example, the _out-cols_ of a `ROWFILTER` node are exactly its _in-cols_.

A node may look at the values of some of its _in-cols_ (but not others) and it may produce some of its _out-cols_ (but not others).
We call these used/produced columns _inputs/outputs_ to distinguish from _in-cols/out-cols_.

We call the _in-cols_ of a node that are actually used its _inputs_.
(For example, the `ROWFILTER` looks at some of its _in-cols_ to decide whether a row passes).
The _inputs_ of node $v$ are labeled $gamma_v^i$ with $0<=i<n_v$, where $n_v$ depends on the spec $theta_v$.

We call the _out-cols_ that are actually produced by a node its _outputs_.
(For example, a `ROWINDEX` node produces one of its _out-cols_, the appended index column).
The _outputs_ of node $v$ are labeled $delta_v^i$ with $0<=i<m_v$, where $m_v$ depends on the spec $theta_v$.

#text(red)[TODO:
`CONCATENATE` and `APPEND` are a bit special.
They use all _in-cols_ and produce all _out-cols_ because they buffer accesses.
If later it becomes known that not all outputs of an `APPEND` are used, we can prune those outputs and the corresponding inputs.
]


== Accesses, access tracing

In practice, every value flowing through the virtual table is a `ReadAccess`
that is _produced_ (created and written) somewhere (e.g. a `SOURCE`) and _used_ (read) elsewhere (e.g. a `ROWFILTER`).

Each node _input_, _output_, _in-col_, or _out-col_ refers to such an _access_.

There is an implied equivalence relation $eq.triple$ between access references:
The in-cols of node $v$ are the out-cols of its spec predecessor $v'=pi(v)$, that is $alpha_v^i eq.triple beta_v'^i$.
A `ROWFILTER` node $u$ passes its in-cols as out-cols to the next node, that is $beta_u^i eq.triple alpha_u^i$.
Each input of node $v$ is an in-col, that is $gamma_v^i eq.triple alpha_v^j$.
And so on.

Every equivalence class in $eq.triple$ contains exactly one producer, that is, the output of the node where the `ReadAccess` is created and written to.

#let uni = $op("union")$
#let find = $op("find")$
The process of programmatically establishing $eq.triple$ is _access tracing_.
This is implemented as a *union-find* algorithm.
We implement *union* to link access reference sets with representatives $alpha, beta$ such that the set leader ($find(alpha)$ or $find(beta)$) is chosen as the "more predecessor one", i.e., the one occuring earlier in the virtual table definition.
This will ultimately lead to the producer being the set leader.

(Note that accesses do not "survive" `APPEND` or `CONCATENATE`. Therefore, $uni$ only happens within a totally ordered sequence of nodes, and consequently "the more predecessor one" is always well-defined.)

The following equivalences hold:
 #text(red)[TODO]

== Data dependencies

A node can only be executed and produce _outputs_ when all its _inputs_ are available.
If node $v$ uses an input that is produced by node $u$ then there is a data dependency edge from $v$ to $u$.

Formally, the set of data dependency edges $D$ is defined as follows:
$
(v,u) in D quad "iff" quad exists (i,j) thick gamma_v^i eq.triple delta_u^j
$


== Control flow dependencies

Order in which nodes are forwarded. Basically: nodes have to be forwarded according to the spec order, with the following exceptions:
- `COLSELECT` are not executed at all (no inputs, no outputs)
- `MAP` nodes can be executed at any point after their inputs are produces and before their outputs are used. This is already ensured by data dependencies.
- Consecutive `ROWFILTER` nodes can be executed in any order.

More formally:\
#let lspec = $scripts(<)_E$
Let $lspec$ be the strict partial order implied by $E$, the set of spec edges.\
Let $Theta^* = Theta without (Mu union Chi)$, the set of specs excluding `MAP` and `COLSELECT`.\
Let $Theta^- = Theta^* without Phi$, the set of specs excluding `MAP`, `COLSELECT`, and `ROWFILTER`.\
The set of control flow edges $F$ is defined as follows:\
Let $theta_v, theta_u in Theta^*$. Then
$
(v,u) in F quad "iff" quad
  (v lspec u) and
  cases(
    (exists.not v' (v lspec v' lspec u) and (theta_v' in Theta^*)) & "if" (theta_v in.not Phi and theta_u in.not Phi),
    (exists.not v' (v lspec v' lspec u) and (theta_v' in Theta^-)) & "if" (theta_v in Phi xor theta_u in Phi),
    bot & "otherwise"
  )
$
equivalent (both not very readable unfortunately):
$
(v,u) in F quad "iff" quad &
  (v lspec u) and (\
  & #h(8mm)    ((theta_v in.not Phi and theta_u in.not Phi) #h(2mm) &&and #h(2mm) exists.not v' (v lspec v' lspec u) and (theta_v' in Theta^*))\
  & #h(4.5mm) or ((theta_v in Phi xor theta_u in Phi) #h(2mm) &&and #h(2mm) exists.not v' (v lspec v' lspec u) and (theta_v' in Theta^-)))
$

The control flow graph $(V,F)$ is "almost a tree":\
Everything is sequential, with branching only on `APPEND` and `CONCATENATE` nodes.
An (otherwise) sequential branch may split into multiple parallel `ROWFILTER` nodes and immediately re-join afterwards.

=== filter-split and filter-join

#let filts = $F^or$
#let filtj = $F^and$
For convenience, before and after parallel `ROWFILTER` nodes, we can put dummy _filter-split_ $filts$ and filter-join $filtj$ nodes whose only purpose is to split and join control flow edges. With this, control flow (between `APPEND` and `CONCATENATE`) can treated as sequential, that is, one edge going into and out of each node.




== Execution ordering

$D$ and $F$ imply a strict partial order $C$ on nodes, where $C$ is the transitive closure of $D union F$.

$D union F$ still has a tree-like structure:
Vertices are `APPEND`, `CONCATENATE`, `SOURCE` and `CONSUMER` nodes.
Vertices are connected with "super-edges" that contain of everything else.
(DAG within super-edge).

To create a concrete execution ordering of a graph,
- pick a sequence in each super-edge that respects $C$, and
- assemble sequences into tree branching at `APPEND` and `CONCATENATE`.

All execution orderings are semantically equivalent, in the sense that they lead to the same _result table_ at the `CONSUMER`.

#text(red)[TODO:\
Unfortunately, it the execution semantics is not trivial to formally define.
If we had a definition, then it should be possible to prove that:
1. the spec order corresponds to a particular execution ordering, and
2. all execution orderings are semantically equivalent, and
2. therefore all execution orderings "do the right thing".
]



== Required accesses and nodes

Recursively define property "_required_" on nodes and accesses:
- The `CONSUMER` node is required.
- If a node is required, then all _inputs_ of the node are required.
- If an _output_ of a node is required, then the node is required.
- If node $v$ is required and $(v,u) in F$, then node $u$ is required.
- #text(red)[TODO: $(v,u) in F$].
- #text(red)[TODO: Special case: `APPEND` and `CONCATENATE` require inputs corresponding to required outputs.]

#text(red)[TODO: generalize to sub-graphs. no `CONSUMER`.]

#pagebreak()

= #text(red)[TODO]

Execution: All nodes in the subgraph, ordered such that all control and data dependencies are satisfied.
(_All nodes_, doesn't matter if they are useful, we'll take care of that separately.)

There should be exactly one node without successor.
If this is not a `CONSUMER` node, then treat subgraph as if a `CONSUMER` would be attached (control flow edge) to that node, using all _out-cols_.

Execution branches at `CONCATENATE` and `APPEND`.
Otherwise, sequential list of nodes.

`ROWFILTER` potentially executes predecessor multiple times.
`SLICE` potententially executes predecessor multiple times.

Otherwise executing a node:
- execute the predecessor (or predecessors in the case of `APPEND` and `CONCATENATE`),
- do some computation on inputs to produce outputs

Some nodes don't do anything (`COLSELECT`, others?) so can be pruned after access tracing.
Ignored for the further formalization.

#pagebreak()

= Subgraphs

Ideas:
- Subgraph with single "output node" (one control flow edge, all _out-cols_) can be treated as a _virtual table source_.
  If additionally there are one or more "input nodes"  (one control flow edge, all _in-cols_), the subgraph can be treated as a single _virtual table operator_.
  Inside a subgraph _in-cols_ and _out-cols_ are not necessary.
  These are only interesting when attaching new virtual table operations (slicing or column selection on cursor, merging partially optimized graphs, ...)
  So we can forget about them when doing modifications (inside) a graph except at the `SOURCE` and `CONSUMER`.

- subgraphs can be replaced by specifying nodes/edges to remove, nodes/edges to add, and mapping for data / control flow crossing into and out of the subgraph.

- All optimization rules can be expressed as subgraph replacements.

- Branches of `APPEND` can be merged if they are "control-flow compatible" (same `SOURCE`s, `ROWFILTER`s, and `SLICE`s).
  There may be varying details (additional `MAP`s, different columns used, additional `ROWINDEX`s).
  We can specify a unification algorithm that produces a single subgraph, such that both "control-flow compatible" branches can be subgraph-replaced with identical copies.
  These branches can then be merged into a single branch.
  (And possibly, the `APPEND` is left with only a single branch and can be eliminated.)



== subgraph replacement

For this, subgraph is more general than previous:

Subgraph $S(U) = (U, G)$ is defined by a subset of nodes $U subset V$ such that
$G subset F$ with $(u, u') in G$ iff $u in U and u' in U$.

The _boundary_ $B(U)$ of subgraph $U$ is the set of all
- inputs $gamma_u^i$ of any node $u in U$ that are produced by nodes $v in.not U$,
- outputs $delta_u^i$ of any node $u in U$ that are used by nodes $v in.not U$,
- nodes $u in U$ where $(u, v) in F$ or $(v, u) in F$ and $v in.not U$\
  (nodes that have control flow links from/to the "outside").

Let's require that a subgraph never contains only part of a parallel `ROWFILTER` section.

Replacement $R$
$
(U, G) scripts(|->)_r (U', G')
$
where $r: B(U) -> B(U`)$ maps boundary elements.

Applying $R$ to $(V,F)$:

$
V |_R &= (V without U) union U' \
F |_R &= (F without G without F_(|U)) union G' union F_(U'|)
$

- remove nodes $U$ and edges $G$
- add nodes $U`$ and edges $G`$
- replace boundary elements




$
a eq.triple^(->) b
$


Maybe abbreviated as 
$
V |_(U |-> U')
$
with control flow edges implied from context.

== subgraph eqivalence

subgraphs are equivalent (wrt subset of outputs) if execution leads to same values on subset of values.





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

= Unification

Two DAGs to match: $G_1, G_2$


#set enum(full: true)
+ Find nodes without _un-linked_ requirements: $C_1, C_2$ 
+ Pick nodes from $C_1, C_2$ and unify:
  + If $v_1 in C_1$ with $theta_v_1 in Sigma union Psi union Xi$:\
    If $exists v_2 in C_2$ such that $theta_v_2 = theta_v_1$, then unify $(v_1, v_2) -> u$.\
    Otherwise fail unification.

  + If $v_1 in C_1$ with $theta_v_1 in Kappa$:\
    If $exists v_2 in C_2$ such that $theta_v_2 = theta_v_1$, then unify $(v_1, v_2) -> u$.\
    Otherwise unify $v_1 -> u$.

  + If $v_2 in C_2$ with $theta_v_2 in Kappa$:\
    Unify $v_2 -> u$.

  + If $v_1 in C_1$ with $theta_v_1 in Mu$:\
    If $exists v_2 in C_2$ such that $theta_v_2 = theta_v_1$ and \
    $forall i exists beta_q$ such that $alpha^i_v_1 -> beta_q and alpha^i_v_2 -> beta_q$,
    then unify $(v_1, v_2) -> u$.\
    Otherwise unify $v_1 -> u$.

  + If $v_2 in C_2$ with $theta_v_2 in Mu$:\
    Unify $v_2 -> u$.



+ next


*Unify $(v_1, v_2) -> u$:*\
Create a node $u$ in the unified DAG.\
Link $v_1 -> u$, $beta^i_v_1 -> beta^i_u$, $beta^r_v_1 -> beta^r_u$.\
Link $v_2 -> u$, $beta^i_v_2 -> beta^i_u$, $beta^r_v_2 -> beta^r_u$.\
Nodes and accesses $v_!, v_2, beta^i_v_1, beta^i_v_2, beta^r_v_1, beta^r_v_2$ are _linked_ now.

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
