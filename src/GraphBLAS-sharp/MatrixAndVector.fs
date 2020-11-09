namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a>() =
    abstract RowCount: int
    abstract ColumnCount: int

    abstract Item: Mask2D option -> Matrix<'a> with get, set
    abstract Item: Mask1D option * int -> Vector<'a> with get, set
    abstract Item: int * Mask1D option -> Vector<'a> with get, set
    abstract Item: int * int -> Scalar<'a> with get, set

    abstract Item: Mask2D option -> Scalar<'a> with set
    abstract Item: Mask1D option * int -> Scalar<'a> with set
    abstract Item: int * Mask1D option -> Scalar<'a> with set

    abstract Mxm: Matrix<'a> -> Mask2D option -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> Semiring<'a> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> Semiring<'a> -> Matrix<'a>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract T: Matrix<'a>

    abstract EWiseAddInplace: Matrix<'a> -> Mask2D option -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Matrix<'a> -> Mask2D option -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask2D option -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (+.*) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y
    static member inline (.+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMultInplace y

and [<AbstractClass>] Vector<'a>() =
    abstract Length: int
    abstract AsArray: 'a[]
    abstract Indices: int list

    abstract Item: Mask1D option -> Vector<'a> with get, set
    abstract Item: int * int -> Scalar<'a> with get, set

    abstract Item: Mask1D option -> Scalar<'a> with set

    abstract Vxm: Matrix<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    abstract EWiseAddInplace: Vector<'a> -> Mask1D option -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Vector<'a> -> Mask1D option -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask1D option -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y
    static member inline (.+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMultInplace y

// двойственная к вектору структура, пока это не явно, но должно легуо приводиться друг в друга
// вместо множества сожно использовать упорядоченную очередь
// можно сдлеать это DU с 1 элементом
and Mask1D(size: int, indexList: int list) =
    let indices = Set.ofList indexList

    member this.GetComplement() =
        let allIndices = List.init size id |> Set.ofList
        let complementIndices = Set.difference allIndices indices |> Set.toList
        Mask1D(size, complementIndices)

    member this.ToArray() =
        indices |> Set.toArray |> Array.sort

    static member OfVector (vector: Vector<'a>) = Mask1D(vector.Length, vector.Indices)
    static member (~~) (mask: Mask1D) = mask.GetComplement()

and Mask2D() = class end

