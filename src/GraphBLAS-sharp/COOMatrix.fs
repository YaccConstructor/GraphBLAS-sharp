namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext
open Microsoft.FSharp.Quotations

/// Describes matrix represented in coordinate format
type COOMatrix<'a when 'a : struct and 'a : equality>(triples: array<'a * int * int>, rowCount: int, columnCount: int) =
    inherit Matrix<'a>()

    new (rowsNumber, columnsNumber) = COOMatrix (Array.empty, rowsNumber, columnsNumber)
    new () = COOMatrix (0, 0)

    member this.Triples: array<'a * int * int> = Array.distinct triples

    override this.RowCount = rowCount
    override this.ColumnCount = columnCount

    override this.Item
        with get (mask: Mask2D) : Matrix<'a> = failwith "Not Implemented"
        and set (mask: Mask2D) (value: Matrix<'a>) = failwith "Not Implemented"
    override this.Item
        with get (vectorMask: Mask1D, colIdx: int) : Vector<'a> = failwith "Not Implemented"
        and set (vectorMask: Mask1D, colIdx: int) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, vectorMask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (rowIdx: int, vectorMask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"

    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> =
            failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Item
        with set (mask: Mask2D) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (vectorMask: Mask1D, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (rowIdx: int, vectorMask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"

    override this.Mxv
        (vector: Vector<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) =

        failwith "Not implemented"

    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"

    override this.T =
        let values, rows, columns = Array.unzip3 this.Triples
        upcast COOMatrix(Array.zip3 values columns rows, this.RowCount, this.ColumnCount)

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
