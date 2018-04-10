# stuff-hs

Mostly fiddling with consensus protocol implementations, simulation testing and linearizability checking.

See also:

* [Linearizability: a correctness condition for concurrent objects](https://dl.acm.org/citation.cfm?id=78972)
* [Linearizability versus Serializability](http://www.bailis.org/blog/linearizability-versus-serializability/)
* [Serializability, linearizability, and locality](https://aphyr.com/posts/333-serializability-linearizability-and-locality)
* [Testing for Linearizability](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf)


## Reordering cases

Process A's operation runs between Ca - Ra, and B's Cb -> Rb. the
linearization point for each Lx is `Cx < Lx < Ex`.

A's op subsumes B's.
```
T   0   1   2   3
A   +----------->
B       +--->
```

In this case, Ca < Cb < Rb < Ra, thus:
Ca < La < Ca
Cb < Lb < Cb

Co the range where La and Lb can be re-ordered is (Ca - Ra) & (Cb - Rb) = (Cb - Rb)

A's op overlaps B's.
```
T   0   1   2   3
A   +------->
B       +------->
```

In this case, Ca < Cb < Ra < Rb, thus:
So the range where La and Lb can be re-ordered is Cb - Ra.


So, we have a happens before relation defined where

a `hb` b = Ra < Cb

Therfore, we can't re-order linearizations around call/returns.
Calls / rets form our "re-ordering buffers", so ... erm.
