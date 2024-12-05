#import "@preview/fletcher:0.5.1" as fletcher: diagram, node, edge

#set page(numbering: "1 / 1")

#align(center, text(17pt)[
  *Virtual Tables Comp-Graph*
])

#align(center)[
  v0.2 \
  05.12.2024 \
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
  [`MAP`],         [$mu in Mu$],           [$M$],
  [`APPENDMAP`],   [$eta in Eta$],         [$H$],
  [`ROWINDEX`],    [$kappa in Kappa$],     [$K$],
  [`OBSERVER`],    [$omicron in Omicron$], [$O$],
)
For example, `SOURCE` specs will be denoted as $sigma_i$, with $sigma_i in Sigma$ the set of all `SOURCE` specs.
`SOURCE` nodes in the compgraph will be labeled $S_j$.

The set of all possible specs is $Theta = Sigma union Chi union Phi union Lambda union Psi union Xi union Omega union Mu union Eta union Kappa union Omicron$.

A virtual table definition (or rather its producing `TableTransform`) translates into a "spec graph":
- Nodes $v in V$ represent `TableTransform`s.
- Edges $e in E$ represent `TableTransform.getPrecedingTransforms()` relationships (pointing from transform to preceeding transform).

For disambigutation, we call the edges _spec edges_.
(Because we will later define different graphs on the same set of nodes).

The _spec graph_ is always a tree.
There is no fork-join allowed: if the same `TableTransform` is re-used in the definition, independent branches are created.
- All leafs are `SOURCE` nodes.
- Interior nodes have exactly one incoming and exactly one outgoing spec edge,
  except `APPEND` and `CONCATENATE`, which may have more than one incoming spec edge.

A node $v = (i_v, theta_v)$ has a unique id $i_v in NN$ and a spec $theta_v in Theta$.\
Note that the multiple nodes can have the same spec (because of fork-join unrolling, or simply because the same spec occurs in different contexts).

An edge $e = (v,v') in E subset V times V$ means $v'$ is a preceeding transform of $v$.


== Spec details

*`SOURCE`:*
$sigma in Sigma$ is a tuple $sigma = (t_sigma, n_sigma)$ where
$t_sigma$ is the UUID of a source table, and
$n_sigma$ is the number of columns in the source table.

*`COLSELECT`:*
$chi in Chi$ is a tuple $chi = (c_chi)$ where
$c_chi in cal(P)(NN)$ is a tuple of input column indices.

*`ROWFILTER`:*
$phi in Phi$ is a tuple $phi = (f_phi, c_phi)$ where
$f_phi$ is the filter function ID, and
$c_phi$ is a tuple of input column indices.

*`SLICE`:*
$lambda in Lambda$ is a tuple $lambda = (j_lambda, k_lambda)$ where $j_lambda$ and $k_lambda$ are row indices.

*`APPEND`:*
$psi in Psi$ is the empty tuple $psi = ()$, i.e., `APPEND` is fully defined by preceeding transforms.

*`CONCATENATE`:*
$xi in Xi$ is the empty tuple $xi = ()$, i.e., `CONCATENATE` is fully defined by preceeding transforms.

*`MAP`*:
$mu in Mu$ is a tuple $mu = (m_mu, c_mu, n_mu)$ where
$m_mu$ is the mapper function ID,
$c_mu$ is a tuple of input column indices, and
$n_mu$ is the number of columns produced by the mapper function.

*`APPENDMAP`*:
is defined identically to `MAP`. That is,
$eta in Eta$ is a tuple $eta = (m_eta, c_eta, n_eta)$ where
$m_eta$ is the mapper function ID,
$c_eta$ is a tuple of input column indices, and
$n_eta$ is the number of columns produced by the mapper function.

*`ROWINDEX`*:
$kappa in Kappa$ is the empty tuple $kappa = ()$, i.e., `ROWINDEX` is fully defined by preceeding transforms.

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

For `APPEND` and `CONCATENATE`, predecessor order matters!\
We use $pi_(i)(v)$ to denote the $i$th predecessor (with $0 <= i < |Pi(v)|$).\
If $|Pi(v)|=1$, we may use $pi(v)$ denotes the single predecessor $pi_(0)(v)$.


== Number of columns

The number of columns of (the virtual table corresponding to the sub-tree rooted in) a node $v$ is

#let ncols = $op("ncols")$
$
ncols(v) := cases(
  n_sigma                   & "if" theta_v = sigma in Sigma     quad &"// SOURCE",
  n_mu                      & "if" theta_v = mu in Mu           quad &"// MAP",
  ncols(pi(v)) + n_eta      & "if" theta_v = eta in Eta         quad &"// APPENDMAP",
  ncols(pi(v)) + 1          & "if" theta_v = kappa in Kappa     quad &"// ROWINDEX",
  |c_chi|                   & "if" theta_v = chi in Chi         quad &"// COLSELECT",
  sum_i ncols(pi_(i)(v))    & "if" theta_v = psi in Psi         quad &"// APPEND",
  ncols(pi_0(v))            & "otherwise"                       quad &"// ROWFILTER, SLICE",
                            &                                        &"// CONCATENATE",
) $


== In/out columns of a node

Every node has _in-cols_ and _out-cols_ that represent the columns "flowing into and out of" the node.

=== In-cols
The node $v$ has $sum_j ncols(pi_(j)(v))$ _in-cols_.\
The _in-cols_ are labeled $alpha_v^(j,i)$ with $0 <= j < |Pi(v)|$) and $0<=i<ncols(pi_(j)(v))$.\
If the node $v$ is not a `SOURCE`, `APPEND`, or `CONCATENATE`,
  then $|Pi(v)|=1$ and
  we may omit the predecessor index $j$ and write $alpha_v^i$ (instead of $alpha_v^(0,i)$).

=== Out-cols
Node $v$ has $ncols(v)$ _out-cols_.\
The _out-cols_ are labeled $beta_v^i$ with $0<=i<ncols(v)$.


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
For `CONCATENATE` and `APPEND`, probably also use the predecessor index: $delta_v^(j,i)$ with $0<=i<m_v$
]

`CONCATENATE` and `APPEND` use all _in-cols_ and produce all _out-cols_, because they need to buffer accesses.
If later it becomes known that not all outputs of an `APPEND` are used, we can prune those outputs and the corresponding inputs.

=== Number of inputs

$
n_v = |gamma_v| := cases(
  |c_mu|                    & "if" theta_v = mu in Mu           quad &"// MAP",
  |c_eta|                   & "if" theta_v = eta in Eta         quad &"// APPENDMAP",
  sum_i ncols(pi_(i)(v))    & "if" theta_v in Psi union Xi      quad &"// APPEND, CONCATENATE",
  0                         & "otherwise"                       quad &"// SOURCE, COLSELECT",
                            &                                        &"// SLICE, ROWINDEX",
) $

=== Number of outputs

$
m_v = |delta_v| := cases(
  n_sigma                   & "if" theta_v in Sigma             quad &"// SOURCE",
  n_mu                      & "if" theta_v in Mu                quad &"// MAP",
  n_eta                     & "if" theta_v in Eta               quad &"// APPENDMAP",
  1                         & "if" theta_v in Kappa             quad &"// ROWINDEX",
  ncols(v)                  & "if" theta_v in Psi union Xi      quad &"// APPEND",
  0                         & "otherwise"                       quad &"// COLFILTER, COLSELECT",
                            &                                        &"// ROWFILTER, SLICE",
) $


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
#text(red)[TODO
 - incols to outcols
 - inputs to incols
 - outputs to outcols
 - predecessor outputs to inputs
]

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
- `MAP` and `APPENDMAP` nodes can be executed at any point after their inputs are produced and before their outputs are used. This is already ensured by data dependencies.
- Consecutive `ROWFILTER` nodes can be executed in any order.

_Implementation note:_\
In practice, `COLSELECT` and `APPENDMAP` never occur in a graph. During graph construction, `COLSELECT` is eliminated and `APPENDMAP` is converted into a `MAP`. 

More formally:\
#let lspec = $scripts(<)_E$
Let $lspec$ be the strict partial order implied by $E$, the set of spec edges.\
Let $Theta^* = Mu union Eta union Chi$, the set of `MAP`, `APPENDMAP`, and `COLSELECT` specs.\
Let $Theta^- = Theta without Theta^*$, the set of specs excluding `MAP`, `APPENDMAP`, and `COLSELECT`.\
The set of control flow edges $F$ is defined as follows:\
Let $theta_v, theta_u in Theta^-$. Then
$
(v,u) in F quad "iff" quad
  (v lspec u) and
  cases(
    forall v' ((v lspec v' lspec u) --> theta_v' in Theta^*) & "if" (theta_v in.not Phi and theta_u in.not Phi),
    forall v' ((v lspec v' lspec u) --> theta_v' in Theta^* union Phi) & "if" (theta_v in Phi xor theta_u in Phi),
    bot & "otherwise"
  )
$
equivalent (both not very readable unfortunately):
$
(v,u) in F quad "iff" quad &
  (v lspec u) and (\
  & #h(8mm)    ((theta_v in.not Phi and theta_u in.not Phi) #h(2mm) &&and #h(2mm) forall v' ((v lspec v' lspec u) --> theta_v' in Theta^*))\
  & #h(4.5mm) or ((theta_v in Phi xor theta_u in Phi) #h(2mm) &&and #h(2mm) forall v' ((v lspec v' lspec u) --> theta_v' in Theta^* union Phi)))
$

The control flow graph $(V,F)$ is "almost a tree":\
Everything is sequential, with branching only on `APPEND` and `CONCATENATE` nodes.
A (otherwise) sequential branch may split into multiple parallel `ROWFILTER` nodes and immediately re-join afterwards.



== Execution ordering

$D$ and $F$ imply a strict partial order $C$ on nodes, where $C$ is the transitive closure of $D union F$.

$D union F$ still has a tree-like structure:
Vertices are `SOURCE`, `APPEND`, and `CONCATENATE` nodes.
Vertices are connected with "super-edges" that contain of everything else.
(DAG within super-edge).

To create a concrete execution ordering of a graph,
- pick a sequence in each super-edge that respects $C$, and
- assemble sequences into tree branching at `APPEND` and `CONCATENATE`.

All execution orderings are semantically equivalent, in the sense that they lead to the same _result table_ at the `CONSUMER`.

#text(red)[TODO:\
Unfortunately, the execution semantics is not trivial to formally define.
If we had a definition, then it should be possible to prove that:
1. the spec order corresponds to a particular execution ordering, and
2. all execution orderings are semantically equivalent, and
2. therefore all execution orderings "do the right thing".
]



== Required accesses and nodes

Recursively define property "_required_" on nodes and accesses:
- All _out-cols_ of the root node of the spec graph are required.
- If an _output_ of a node is required, then the node is required.
- If node $v$ is required and $(v,u) in F$, then node $u$ is required.
- If a node is required, then all _inputs_ of the node are required.\
  #text(red)[TODO:\ Special case: `APPEND` and `CONCATENATE` only require inputs corresponding to required outputs.]


= #text(red)[TODO Execution]

Execution: All nodes in the subgraph, ordered such that all control and data dependencies are satisfied.
(_All nodes_, doesn't matter if they are useful, we'll take care of that separately.)

There should be exactly one node without successor. Execution starts at this node.

Execution branches at `CONCATENATE` and `APPEND`.
Otherwise, sequential list of nodes.

`ROWFILTER` potentially executes predecessor multiple times.
`SLICE` potententially executes predecessor multiple times.

Otherwise, executing a node:
- execute the predecessor (or predecessors in the case of `APPEND` and `CONCATENATE`),
- do some computation on inputs to produce outputs


#pagebreak()

= Subgraphs

Ideas:
- Subgraph with single "output node" (one control flow edge, all _out-cols_) can be treated as a _virtual table source_.
  If additionally there are one or more "input nodes"  (one control flow edge, all _in-cols_), the subgraph can be treated as a single _virtual table operator_.
  Inside a subgraph _in-cols_ and _out-cols_ are not necessary.
  These are only interesting when attaching new virtual table operations (slicing or column selection on cursor, merging partially optimized graphs, ...)
  So we can forget about them when doing modifications (inside) a graph except at the `SOURCE` and `CONSUMER`.

- Subgraphs can be replaced by specifying nodes/edges to remove, nodes/edges to add, and mapping for data / control flow crossing into and out of the subgraph.

- All optimization rules can be expressed as subgraph replacements.

- Branches of `APPEND` can be merged if they are "control-flow compatible".
  (And possibly, the `APPEND` is left with only a single branch and can be eliminated.)



== Subgraph replacement

A subgraph $S(U) = (U, F_U)$ of $(V,F)$ is defined by a subset of nodes $U subset V$.\
$U$ implies the set of fully contained control flow edges $F_U subset F$, i.e.,
$
F_U = { (u, u') | (u, u') in F and u in U and u' in U}.
$

The _input boundary_ $Gamma_U$ is the set of inputs of nodes $U$ that are produced by nodes outside of $U$, i.e.,
$
Gamma_U = {gamma_u^i | u in U and gamma_u^i eq.triple delta_v^j and v in.not U}.
$

The _output boundary_ $Delta_U$ is the set of outputs of nodes $U$ that are used by nodes outside of $U$, i.e.,
$
Delta_U = {delta_u^i | u in U and delta_u^i eq.triple gamma_v^j and v in.not U}.
$

The _in-flow boundary_ $I(U) subset F$ is the set of control flow edges crossing into the subgraph, i.e.,
$
I(U) = {(v,u) | (v,u) in F and v in.not U and u in U}.
$

The _out-flow boundary_ $O(U) subset F$ is the set of control flow edges crossing out of the subgraph, i.e.,
$
O(U) = {(u, v) | (u, v) in F and v in.not U and u in U}.
$

The _boundary_ $B(U) subset U$ is the set of nodes that have edges crossing into or out of the subgraph, i.e.,
$
B(U) = {u | u in U and exists (v in.not U) thick (u,v) in F or (v,u) in F} = {u | (u,v) in O(U) or (v,u) in I(U)}.
$

Subgraphs $S(U)$ can be replaced with a graph $(U',F')$.
We can express comp graph optimization rules as a series of appropriate subgraph replacements that do not change the overall semantics of the graph.

Replacing $S(U) = (U, F_U)$ requires $(U',F')$ which replace the nodes and fully contained edges,
and a mapping $r$ for "stitching" the boundary:
- $r_Gamma: Gamma_U -> Gamma(U')$,
- $r_Delta: Delta_U -> Delta(U')$,
- $r_I: I(U) -> {(v, u') | v in F without U and u' in U'}$, and
- $r_O: O(U) -> {(u', v) | v in F without U and u' in U'}$.


= Unification

Branches of `APPEND` can be merged if they are "control-flow compatible".
That means, they have the same `SOURCE`s, `ROWFILTER`s, and `SLICE`s in the same (partial) order.
Other details may vary: Some `MAP` and `ROWINDEX` nodes might occur in only one of the branches.

Such branches can be merged into a single branch.
(And possibly, the `APPEND` is left with only a single branch and can be eliminated.)

The following unification algorithm produces a single subgraph, such that both branches can be subgraph-replaced with identical copies of the unified graph
(or fails if the branches are not "control-flow compatible").

#text(red)[
  For now, assume that the matched subgraphs have no `APPEND` or `CONCATENATE`.
  This can be built on top, but requires backtracking to find corresponding branches.
  ]

*Algorithm: Unification* \
*Input:* subgraphs $V_1, V_2 subset V$ \
*Output* $(U`, F`), r_Delta_1, r_Delta_2, r_U_1, r_U_2$ \
#set enum(full: true)
+ Initialize
  $U' := emptyset$,
  $F' := emptyset$,
  $r_Delta_1 := emptyset$,
  $r_Delta_2 := emptyset$,
  $r_U_1 := emptyset$,
  $r_U_2 := emptyset$
+ Repeat until all nodes in $V_1$ and $V_2$ have a corresponding node in $U'$:
  + Find candidates from $U_1$ for adding to the unified graph,
    $
      C_1 = {v in U_1 | (forall i thick r_Delta_1(gamma^i_v_1) eq.not emptyset) and (forall (v,u) in F thick r_U_1(u) eq.not emptyset)}
    $
    and similarly candidates $C_2$ from $U_2$
  + Find matching candidates $v_1 in C_1, v_2 in C_2$ such that
    $
    theta_v_1 &= theta_v_2 ", and" \
    forall i thick r_Delta_1(gamma^i_v_1) & eq.triple r_Delta_2(gamma^i_v_2).
    $
    (I.e., the nodes have matching specs and matching inputs.)
  + If a match was found, add a new unified node $u$ with $theta_u = theta_v_1$:
    $
      U' &:= U' union {u} \
      F' &:= F' union {(u, v) | (u', v') in F and (r_U_1(u') = u) and (r_U_1(v') = v)} \
      r_Delta_1(delta^i_v_1) &:= delta^i_u quad (forall i)\
      r_U_1(v_1) &:= u \
      F' &:= F' union {(u, v) | (u', v') in F and (r_U_2(u') = u) and (r_U_2(v') = v)} \
      r_Delta_2(delta^i_v_2) &:= delta^i_u quad (forall i)\
      r_U_2(v_2) &:= u \
    $
  + If no match was found:
    + If there is a `ROWINDEX` in one of the candidate sets, add a unified node for that.
      For example, assume $v_1 in C_1$ with $theta_v_1 in Kappa$, then add $u$ with $theta_u = theta_v_1$:
      $
        U' &:= U' union {u} \
        F' &:= F' union {(u, v) | (u', v') in F and (r_U_1(u') = u) and (r_U_1(v') = v)} \
        r_Delta_1(delta^i_v_1) &:= delta^i_u quad (forall i)\
        r_U_1(v_1) &:= u \
      $
    + Else-if there is a `MAP` in one of the candidate sets, add a unified node for that.
    + Else fail. ($V_1$ and $V_2$) are not compatible.


// #pagebreak()
= Optimizations

The idea is to replace subgraphs such that the overall semantics of the graph is not changed.

#text(red)[*TODO:*]
Think through these optimization steps.\
How do they work by just relinking access usage?\
Is there anything in input/output linking etc that would prevent them?\
Are input/output still meaningful after applying them?\
Can we still recover a spec graph after applying them?\

- eliminateRowIndexes
- eliminateAppends
- mergeSlices
- #strike[mergeRowIndexSiblings]
- mergeRowIndexSequence
- #strike[moveSlicesBeforeObserves]
- moveSlicesBeforeAppends
- moveSlicesBeforeRowIndexes
- moveSlicesBeforeConcatenates
- eliminateSingletonConcatenates
