namespace GraphBLAS.FSharp.Backend.Algorithms

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

module internal PageRank =
    let alpha = 0.85f
    let accuracy = 0.00000001f

    let countOutDegree (clContext: ClContext) workGroupSize =

        let one =
            <@ fun (x: float32 option) (_: int option) ->
                let mutable res = 0

                match x with
                | Some _ -> res <- 1
                | None -> ()

                if res = 0 then None else Some res @>

        let spMV =
            Operations.SpMV.runTo ArithmeticOperations.intSumOption one clContext workGroupSize

        let zeroCreate =
            GraphBLAS.FSharp.ClArray.zeroCreate clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<float32>) ->
            let outDegree: ClArray<int option> =
                zeroCreate queue DeviceOnly matrix.ColumnCount

            spMV queue matrix outDegree outDegree

            outDegree

    let prepareMatrix (clContext: ClContext) workGroupSize =

        let alpha = 0.85f

        let op =
            <@ fun (x: float32 option) y ->
                let mutable res = 0.0f

                match x, y with
                | Some _, Some y -> res <- alpha / (float32 y)
                | Some _, None -> ()
                | None, Some _ -> ()
                | None, None -> ()

                if res = 0.0f then None else Some res @>

        //TODO: generalize to map2 Matrix x Vector
        let multiply =
            <@ fun (range: Range1D) (numberOfRows: int) (matrixRowPointers: ClArray<int>) (matrixValues: ClArray<float32>) (vectorValues: ClArray<int option>) (resultMatrixValues: ClArray<float32>) ->

                let i = range.GlobalID0
                let li = range.LocalID0
                let group = i / workGroupSize

                if group < numberOfRows then
                    let rowStart = matrixRowPointers.[group]
                    let rowEnd = matrixRowPointers.[group + 1]

                    let vectorValue = vectorValues.[group]
                    let mutable index = rowStart + li

                    while index < rowEnd do
                        let matrixValue = matrixValues.[index]
                        let resultValue = (%op) (Some matrixValue) vectorValue

                        match resultValue with
                        | Some v -> resultMatrixValues.[index] <- v
                        | None -> () //This should not be reachable

                        index <- index + workGroupSize @>

        let countOutDegree = countOutDegree clContext workGroupSize

        let copy =
            GraphBLAS.FSharp.ClArray.copy clContext workGroupSize

        let transposeInPlace =
            Matrix.CSR.Matrix.transposeInPlace clContext workGroupSize

        let multiply = clContext.Compile multiply

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<float32>) ->

            match matrix with
            | ClMatrix.CSR matrix ->

                let outDegree = countOutDegree queue matrix

                let resultValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, matrix.Values.Length)

                let kernel = multiply.GetKernel()

                let ndRange =
                    Range1D.CreateValid(matrix.RowCount * workGroupSize, workGroupSize)

                queue.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                matrix.RowCount
                                matrix.RowPointers
                                matrix.Values
                                outDegree
                                resultValues)
                )

                queue.Post(Msg.CreateRunMsg<_, _> kernel)

                outDegree.Free queue

                let newMatrix =
                    { Context = clContext
                      RowCount = matrix.RowCount
                      ColumnCount = matrix.ColumnCount
                      RowPointers = copy queue DeviceOnly matrix.RowPointers
                      Columns = copy queue DeviceOnly matrix.Columns
                      Values = resultValues }

                transposeInPlace queue DeviceOnly newMatrix
                |> ClMatrix.CSR
            | _ -> failwith "Not implemented"

    let run (clContext: ClContext) workGroupSize =

        let alpha = 0.85f
        let accuracy = 0.00000001f

        let minusAndSquare = ArithmeticOperations.minusAndSquare
        let plus = ArithmeticOperations.float32SumOption
        let mul = ArithmeticOperations.float32MulOption

        let spMVTo =
            Operations.SpMVInPlace plus mul clContext workGroupSize

        let addToResult =
            GraphBLAS.FSharp.Vector.map2InPlace plus clContext workGroupSize

        let subtractAndSquare =
            GraphBLAS.FSharp.Vector.map2To minusAndSquare clContext workGroupSize

        let reduce =
            GraphBLAS.FSharp.Vector.reduce <@ (+) @> clContext workGroupSize

        let create =
            GraphBLAS.FSharp.Vector.create clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<float32>) ->

            let vertexCount = matrix.RowCount

            //None is 0
            let mutable rank =
                create queue DeviceOnly vertexCount Dense None

            let mutable prevRank =
                create queue DeviceOnly vertexCount Dense (Some(1.0f / (float32 vertexCount)))

            let mutable errors =
                create queue DeviceOnly vertexCount Dense None

            let addition =
                create queue DeviceOnly vertexCount Dense (Some((1.0f - alpha) / (float32 vertexCount)))

            let mutable error = accuracy + 0.1f

            let mutable i = 0

            while error > accuracy do
                i <- i + 1

                // rank = matrix*rank + (1 - alpha)/N
                spMVTo queue matrix prevRank rank
                addToResult queue rank addition

                // error
                subtractAndSquare queue rank prevRank errors
                error <- sqrt <| (reduce queue errors).ToHostAndFree queue

                //Swap vectors
                let temp = rank
                rank <- prevRank
                prevRank <- temp

            prevRank.Dispose queue
            errors.Dispose queue
            addition.Dispose queue

            rank
