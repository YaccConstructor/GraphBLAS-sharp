namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext

type COOMatrix<'a when 'a : struct and 'a : equality>(triples: array<'a*int*int>, rowsCount: int, columnsCount: int) =
    inherit Matrix<'a>()

    member this.Triples: array<'a*int*int> =
            //remove duplicates
            Array.filter (fun x -> triples |> Array.filter (fun y -> x = y) |> Array.length = 1) triples

    override this.RowCount = rowsCount
    override this.ColumnCount = columnsCount

    override this.Item
        with get (mask: Mask2D<'a>) : Matrix<'a> = failwith "Not Implemented"
        and set (mask: Mask2D<'a>) (value: Matrix<'a>) = failwith "Not Implemented"
    override this.Item
        with get (vectorMask: Mask1D<'a>, colIdx: int) : Vector<'a> = failwith "Not Implemented"
        and set (vectorMask: Mask1D<'a>, colIdx: int) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, vectorMask: Mask1D<'a>) : Vector<'a> = failwith "Not Implemented"
        and set (rowIdx: int, vectorMask: Mask1D<'a>) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (mask: Mask2D<'a>) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (vectorMask: Mask1D<'a>, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (rowIdx: int, vectorMask: Mask1D<'a>) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.T = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
