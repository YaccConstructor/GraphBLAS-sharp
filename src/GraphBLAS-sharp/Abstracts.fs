namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

(*
    везде вместо того, чтобы возвращать unit, можно возвращать измененный объект,
    но нужно подумать, насколько создание нового объекта лучше/хуже изменения существующего
    копирование большмх матриц явно хуже и зачем, а вот Clear сделать не inplace мб имеет смысл

    везде методы, тк проперти не соответсвуют концепции отсутствия вычислений,
    хотя выглядеть будет оч плохо

    можно все операции, в которых не меняется структура объекта, сделать inplace
    (хотя, если операция затрагивает весь объект, то все равно читать и писать,
     единственно, что памяти единовременно в 2 раза больше потребуется)

    теперь есть единственный метод GetMask (без GetComplemented)
    тк это теперь не свойство, то требует аргумента, а значит можно передавать isComplemented

    методы Extract и Assign, которые возвращают/присваивают подграф, семантически отличаются от тех, что в C
    здесь возвращаемый подграф (подматрица) всегда того же размера, что и изначальная матрица
    (т.е в матрице смежности вершины графа не удалаются, а только ребра)
    поэтому перед операцией нужно сравнивать размерности матрицы и маски, по которой мы получаем подграф
    чтобы удалить еще и вершины, можно потом сделать resize

    у вектора размерность теперь называется size, а не length, потому что идейно вектор -- набор вершин
    + в C API тоже size

    метод Prune можно переименовать в Select или Filter

    нужно выяснить, как curried методы интеропятся с C#

    возможно, стоит отказаться от перегрузок Extract и Assign, чтобы сделать их curried,
    тем самым, избавившись от скобок при вызове

    можно все методы сделать как методы C# (без curried), а рядом положить модуль с curried функциями
    это нужно, для более гибкого интерфейся и лучшего интеропа с C#

    пара массивов лучше ложиться на opencl чем массив пар. Поэтому изменились аргументы Mask1D
    стоит это учесть и в дугих местах. Например разреженный вектор должен принимать пару массивов вместо массива пар
*)

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

    abstract Clear: unit -> OpenCLEvaluation<unit>
    abstract Copy: unit -> OpenCLEvaluation<Matrix<'a>>
    abstract Resize: int -> int -> OpenCLEvaluation<Matrix<'a>>
    abstract GetNNZ: unit -> OpenCLEvaluation<int>
    abstract GetTuples: unit -> OpenCLEvaluation<{| Rows: int[]; Columns: int[]; Values: 'a[] |}>
    abstract GetMask: ?isComplemented: bool -> OpenCLEvaluation<Mask2D option>

    abstract Extract: Mask2D option -> OpenCLEvaluation<Matrix<'a>>
    abstract Extract: (Mask1D option * int) -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: (int * Mask1D option) -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: (int * int) -> OpenCLEvaluation<Scalar<'a>>
    abstract Assign: Mask2D option * Matrix<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (Mask1D option * int) * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * Mask1D option) * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * int) * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: Mask2D option * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (Mask1D option * int) * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * Mask1D option) * Scalar<'a> -> OpenCLEvaluation<unit>

    abstract Mxm: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract Mxv: Vector<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract Apply: Mask2D option -> UnaryOp<'a, 'b> -> OpenCLEvaluation<Matrix<'b>>
    abstract Prune: Mask2D option -> UnaryOp<'a, bool> -> OpenCLEvaluation<Matrix<'a>>
    abstract ReduceIn: Mask1D option -> Monoid<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract ReduceOut: Mask1D option -> Monoid<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract Reduce: Monoid<'a> -> OpenCLEvaluation<Scalar<'a>>
    abstract Transpose: unit -> OpenCLEvaluation<Matrix<'a>>
    abstract Kronecker: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (@.) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (@.) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y


and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(size: int) =
    abstract Size: int
    default this.Size = size

    abstract Clear: unit -> OpenCLEvaluation<unit>
    abstract Copy: unit -> OpenCLEvaluation<Vector<'a>>
    abstract Resize: int -> OpenCLEvaluation<Vector<'a>>
    abstract GetNNZ: unit -> OpenCLEvaluation<int>
    abstract GetTuples: unit -> OpenCLEvaluation<{| Indices: int[]; Values: 'a[] |}>
    abstract GetMask: ?isComplemented: bool -> OpenCLEvaluation<Mask1D option>

    abstract Extract: Mask1D option -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: int -> OpenCLEvaluation<Scalar<'a>>
    abstract Assign: Mask1D option * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: int * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: Mask1D option * Scalar<'a> -> OpenCLEvaluation<unit>

    abstract Vxm: Matrix<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> OpenCLEvaluation<Vector<'b>>
    abstract Prune: Mask1D option -> UnaryOp<'a, bool> -> OpenCLEvaluation<Vector<'a>>
    abstract Reduce: Monoid<'a> -> OpenCLEvaluation<Scalar<'a>>

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (@.) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y


and Mask1D(indices: int[], size: int, isComplemented: bool) =
    member this.Indices = indices
    member this.Size = size
    member this.IsComplemented = isComplemented


and Mask2D(rowIndices: int[], columnIndices: int[], rowCount: int, columnCount: int, isComplemented: bool) =
    member this.RowIndices = rowIndices
    member this.ColumnIndices = columnIndices
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount
    member this.IsComplemented = isComplemented
