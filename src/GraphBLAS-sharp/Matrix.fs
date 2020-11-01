namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a>() =
    abstract RowCount: int
    abstract ColumnCount: int

    // abstract member Add: Matrix<'a> -> Matrix<'a>
    // abstract member Mult: Matrix<'a> -> Matrix<'a>
    // abstract Dot: Matrix<'a> -> ComputationalContext<'a> -> Matrix<'a>
    abstract Dot: Vector<'a> -> ComputationalContext<'a> -> Vector<'a>
