namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext
open Microsoft.FSharp.Quotations

/// Describes matrix represented in coordinate format
type COOMatrix<'a when 'a : struct and 'a : equality>(triples: array<'a*int*int>, rowCount: int, columnCount: int) =
    inherit Matrix<'a>()

    new (rowsNumber, columnsNumber) = COOMatrix (Array.empty, rowsNumber, columnsNumber)
    new () = COOMatrix (0, 0)

    member this.Triples: array<'a*int*int> =
            //remove duplicates
            Array.filter (fun x -> triples |> Array.filter (fun y -> x = y) |> Array.length = 1) triples

    override this.RowCount = rowCount
    override this.ColumnCount = columnCount

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

    // Multiply by vector without a mask
    member this.MxvNoneMask
        (vector: Vector<'a>)
        (semiring: Semiring<'a>) : Vector<'a> =

        let matrixRowCount = this.RowCount

        let plus = !> semiring.PlusMonoid.Append
        let mult = !> semiring.Times

        let values, rows, columns = Array.unzip3 this.Triples
        let resultVector = Array.zeroCreate<'a> matrixRowCount
        let command =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: array<'a>)
                    (valuesBuffer: array<'a>)
                    (rowsBuffer: array<int>)
                    (columnsBuffer: array<int>)
                    (vectorBuffer: array<'a>) ->

                    let i = ndRange.GlobalID0
                    let mutable localResultBuffer = resultBuffer.[i]
                    for j in 0 .. valuesBuffer.Length - 1 do
                        if rowsBuffer.[j] = i then
                            localResultBuffer <- (%plus) localResultBuffer
                                ((%mult) valuesBuffer.[j] vectorBuffer.[columnsBuffer.[j]])

                    resultBuffer.[i] <- localResultBuffer
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
        let ndRange = _1D(matrixRowCount)
        kernelPrepare
            ndRange
            resultVector
            values
            rows
            columns
            vector.AsArray
        currentContext.CommandQueue.Add(kernelRun ()) |> ignore
        currentContext.CommandQueue.Add(resultVector.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Finish() |> ignore

        upcast DenseVector(resultVector)

    // Multiply by vector with a mask
    member this.MxvMask
        (vector: Vector<'a>)
        (maskVector: Vector<'a>)
        (semiring: Semiring<'a>)
        (predicate: Expr<'a -> 'a -> bool>) : Vector<'a> =

        let matrixRowCount = this.RowCount

        let plus = !> semiring.PlusMonoid.Append
        let mult = !> semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let values, rows, columns = Array.unzip3 this.Triples
        let resultVector = Array.init matrixRowCount (fun _ -> zero)
        let command =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: array<'a>)
                    (valuesBuffer: array<'a>)
                    (rowsBuffer: array<int>)
                    (columnsBuffer: array<int>)
                    (vectorBuffer: array<'a>)
                    (maskVectorBuffer: array<'a>) ->

                    let i = ndRange.GlobalID0
                    if (%predicate) maskVectorBuffer.[i] zero then
                    // if maskVectorBuffer.[i] <> zero then
                        let mutable localResultBuffer = resultBuffer.[i]
                        for j in 0 .. valuesBuffer.Length - 1 do
                            if rowsBuffer.[j] = i then
                                localResultBuffer <- (%plus) localResultBuffer
                                    ((%mult) valuesBuffer.[j] vectorBuffer.[columnsBuffer.[j]])

                        resultBuffer.[i] <- localResultBuffer
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
        let ndRange = _1D(matrixRowCount)
        kernelPrepare
            ndRange
            resultVector
            values
            rows
            columns
            vector.AsArray
            maskVector.AsArray
        currentContext.CommandQueue.Add(kernelRun ()) |> ignore
        currentContext.CommandQueue.Add(resultVector.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Finish() |> ignore

        upcast DenseVector(resultVector)

    override this.Mxv
        (vector: Vector<'a>)
        (mask: Mask1D<'a>)
        (semiring: Semiring<'a>) =

        if this.ColumnCount <> vector.Length then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.ColumnCount vector.Length)

        match mask with
            | Mask1D maskVector ->
                if maskVector.Length <> vector.Length then
                    invalidArg
                         "mask"
                         (sprintf "Argument has invalid dimension. Need %i, but given %i" vector.Length maskVector.Length)

                this.MxvMask vector maskVector semiring <@ ( <> ) @>
            | Complemented1D maskVector ->
                if maskVector.Length <> vector.Length then
                    invalidArg
                         "mask"
                         (sprintf "Argument has invalid dimension. Need %i, but given %i" vector.Length maskVector.Length)

                this.MxvMask vector maskVector semiring <@ ( = ) @>
            | Mask1D.None -> this.MxvNoneMask vector semiring

    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"

    override this.T =
        let values, rows, columns = Array.unzip3 this.Triples

        let command =
            <@
                fun (ndRange: _1D)
                    (rowsBuffer: array<int>)
                    (columnsBuffer: array<int>) ->

                    let i = ndRange.GlobalID0
                    let buff = rowsBuffer.[i]
                    rowsBuffer.[i] <- columnsBuffer.[i]
                    columnsBuffer.[i] <- buff
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
        let ndRange = _1D(this.Triples.Length)
        kernelPrepare
            ndRange
            rows
            columns
        currentContext.CommandQueue.Add(kernelRun ()) |> ignore
        currentContext.CommandQueue.Add(rows.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Add(columns.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Finish() |> ignore

        upcast COOMatrix(Array.zip3 values rows columns, this.RowCount, this.ColumnCount)

        // Second variant
        //upcast COOMatrix(Array.map (fun (a, i, j) -> (a, j, i)) this.Triples, this.RowCount, this.ColumnCount)

        // Third variant
        //let values, rows, columns = Array.unzip3 this.Triples
        //upcast COOMatrix(Array.zip3 values columns rows, this.RowCount, this.ColumnCount)

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
