namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a>() =
    abstract RowCount: int
    abstract ColumnCount: int

    // abstract Mxm: Matrix<'a> -> ComputationalContext<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> ComputationalContext<'a> -> Vector<'a>
    // abstract EWiseMult: Matrix<'a> -> ComputationalContext<'a> -> Matrix<'a>
    // abstract EWiseAdd: Matrix<'a> -> ComputationalContext<'a> -> Matrix<'a>
    // abstract T: Matrix<'a>
