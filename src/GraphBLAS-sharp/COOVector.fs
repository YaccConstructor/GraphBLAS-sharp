namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext

type COOVector<'a when 'a : struct and 'a : equality>(dyads: array<'a*int>, length: int) =
    inherit Vector<'a>()

    new(length) = COOVector(Array.empty, length)
    new() = COOVector(0)

    member this.Dyads =
        //remove duplicates
        Array.filter (fun x -> dyads |> Array.filter (fun y -> x = y) |> Array.length = 1) dyads

    override this.Length = length
    override this.AsArray = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D<'a>) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D<'a>) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (mask: Mask1D<'a>) (value: Scalar<'a>) = failwith "Not Implemented"

    // Multiply by matrix in coordinate format with a regular mask
    member this.VxCOOmMask
        (matrix: COOMatrix<'a>)
        (maskVector: Vector<'a>)
        (semiring: Semiring<'a>) : Vector<'a> =

        let matrixColumnCount = matrix.ColumnCount

        let plus = !> semiring.PlusMonoid.Append
        let mult = !> semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let vectorValues, indices = Array.unzip this.Dyads
        let matrixValues, rows, columns = Array.unzip3 matrix.Triples
        let resultValues = Array.init matrixColumnCount (fun _ -> zero)
        let resultIndices = Array.init matrixColumnCount id
        let command =
            <@
                fun (ndRange: _1D)
                    (resultValuesBuffer: array<'a>)
                    (matrixValuesBuffer: array<'a>)
                    (rowsBuffer: array<int>)
                    (columnsBuffer: array<int>)
                    (vectorValuesBuffer: array<'a>)
                    (vectorIndicesBuffer: array<int>)
                    (maskVectorBuffer: array<'a>) ->

                    let i = ndRange.GlobalID0
                    if maskVectorBuffer.[i] <> zero then
                        let mutable localResultBuffer = resultValuesBuffer.[i]
                        for j in 0 .. vectorValuesBuffer.Length - 1 do
                            for k in 0 .. matrixValuesBuffer.Length - 1 do
                                if rowsBuffer.[k] = vectorIndicesBuffer.[j] && columnsBuffer.[k] = i then
                                    localResultBuffer <- (%plus) localResultBuffer
                                        ((%mult) vectorValuesBuffer.[j] matrixValuesBuffer.[k])
                        resultValuesBuffer.[i] <- localResultBuffer
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
        let ndRange = _1D(matrixColumnCount)
        kernelPrepare
            ndRange
            resultValues
            matrixValues
            rows
            columns
            vectorValues
            indices
            maskVector.AsArray
        currentContext.CommandQueue.Add(kernelRun ()) |> ignore
        currentContext.CommandQueue.Add(resultValues.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Finish() |> ignore

        upcast COOVector(Array.zip resultValues resultIndices, matrixColumnCount)

    member this.VxCOOm
        (matrix: COOMatrix<'a>)
        (mask: Mask1D<'a>)
        (semiring: Semiring<'a>) : Vector<'a> =

        if matrix.RowCount <> this.Length then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length matrix.RowCount)

        match mask with
            | Mask1D maskVector ->
                if maskVector.Length <> this.Length then
                    invalidArg
                         "mask"
                         (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length maskVector.Length)

                this.VxCOOmMask matrix maskVector semiring
            | _ -> failwith "Not Implemented"

    override this.Vxm
        (matrix: Matrix<'a>)
        (mask: Mask1D<'a>)
        (semiring: Semiring<'a>) =

        match matrix with
            | :? COOMatrix<'a> -> this.VxCOOm (downcast matrix) mask semiring
            | _ -> failwith "Not Implemented"

    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"

    override this.Reduce
        (monoid: Monoid<'a>) =

        let nonZeroValuesCount = this.Dyads.Length

        let mutable expNonZeroValuesCount = 1
        let mutable exponent = 0

        while expNonZeroValuesCount < nonZeroValuesCount do
            expNonZeroValuesCount <- expNonZeroValuesCount * 2
            exponent <- exponent + 1
        expNonZeroValuesCount <- expNonZeroValuesCount / 2

        let plus = !> monoid.Append

        let values, _ = Array.unzip this.Dyads
        let command =
            <@
                fun (ndRange: _1D)
                    (valuesBuffer: array<'a>) ->

                let i = ndRange.GlobalID0
                valuesBuffer.[i] <- (%plus) valuesBuffer.[i] valuesBuffer.[i + expNonZeroValuesCount]
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command

        for i in 0 .. exponent - 1 do
            let ndRange = _1D(expNonZeroValuesCount - 1)
            kernelPrepare
                ndRange
                values
            currentContext.CommandQueue.Add(kernelRun ()) |> ignore
            currentContext.CommandQueue.Add(values.ToHost currentContext.Provider) |> ignore
            currentContext.CommandQueue.Finish() |> ignore

            expNonZeroValuesCount <- expNonZeroValuesCount / 2

        Scalar values.[0]

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
