namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

type UnaryOp<'a, 'b> = UnaryOp of Expr<'a -> 'b>
type BinaryOp<'a, 'b, 'c> = BinaryOp of Expr<'a -> 'b -> 'c>

type ClosedUnaryOp<'a> = ClosedUnaryOp of Expr<'a -> 'a>
type ClosedBinaryOp<'a> = ClosedBinaryOp of Expr<'a -> 'a -> 'a>

/// Magma with associative (magma is set with closed binary operator)
type ISemigroup<'a> =
    abstract Op : ClosedBinaryOp<'a>

/// Semigroup with identity
type IMonoid<'a> =
    abstract Plus : ClosedBinaryOp<'a>
    abstract Zero : 'a

/// Monoid with associative binary operator,
/// for wich Zero is annihilator
type ISemiring<'a> =
    abstract Zero : 'a
    abstract Plus : ClosedBinaryOp<'a>
    abstract Times : ClosedBinaryOp<'a>

type Semigroup<'a> =
    { AssociativeOp: ClosedBinaryOp<'a> }

    interface ISemigroup<'a> with
        member this.Op = this.AssociativeOp

type Monoid<'a> =
    { AssociativeOp: ClosedBinaryOp<'a>
      Identity: 'a }

    interface ISemigroup<'a> with
        member this.Op = this.AssociativeOp

    interface IMonoid<'a> with
        member this.Plus = this.AssociativeOp
        member this.Zero = this.Identity

type Semiring<'a> =
    { PlusMonoid: Monoid<'a>
      TimesSemigroup: Semigroup<'a> }

    interface IMonoid<'a> with
        member this.Zero = this.PlusMonoid.Identity
        member this.Plus = this.PlusMonoid.AssociativeOp

    interface ISemiring<'a> with
        member this.Times = this.TimesSemigroup.AssociativeOp
        member this.Zero = this.PlusMonoid.Identity
        member this.Plus = this.PlusMonoid.AssociativeOp
