module Backend.SPGEMM

open FsCheck
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.Generators
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "SPGEMM.Tests"

let context =
    let deviceType = ClDeviceType.Default
    let platformName = ClPlatform.Any
    ClContext(platformName, deviceType)

let getMatricesToMultiply generator size isZero mFormat =
    let gen = generator |> Arb.toGen
    let m1, m2 = (Gen.sample (abs size) 1 gen).[0]

    let mtx1, mtx2 =
        createMatrixFromArray2D mFormat m1 isZero, createMatrixFromArray2D mFormat m2 isZero

    mtx1, mtx2, m1, m2

let checkResult times plus zero (baseMtx1: 'a [,]) (baseMtx2: 'a [,]) (actual: Matrix<'a>) =
    let rows = Array2D.length1 baseMtx1
    let columns = Array2D.length2 baseMtx2
    Expect.equal columns actual.ColumnCount "The number of columns should be the same."
    Expect.equal rows actual.RowCount "The number of rows should be the same."

    let expected = Array2D.create rows columns zero

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            for k in 0 .. Array2D.length2 baseMtx1 - 1 do
                expected.[i, j] <- plus expected.[i, j] <| times baseMtx1.[i, k] baseMtx2.[k, j]

    let actual2D =
        Array2D.create actual.RowCount actual.ColumnCount zero

    let actual2D =
        match actual with
        | MatrixCOO actual ->
            for i in 0 .. actual.Rows.Length - 1 do
                actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]

            actual2D
        | MatrixCSR actual ->
            let rowIndices =
                Array.create actual.ColumnIndices.Length 0

            for i in 0 .. actual.RowCount - 1 do
                if i < actual.RowCount - 1 then
                    let rowStart = actual.RowPointers.[i]
                    let rowEnd = actual.RowPointers.[i + 1]
                    let rowLength = rowEnd - rowStart

                    for j in 0 .. rowLength - 1 do
                        rowIndices.[rowStart + j] <- i
                else
                    let rowStart = actual.RowPointers.[actual.RowCount - 1]
                    let rowLength = rowIndices.Length - rowStart

                    for j in 0 .. rowLength - 1 do
                        rowIndices.[rowStart + j] <- i

            for i in 0 .. rowIndices.Length - 1 do
                actual2D.[rowIndices.[i], actual.ColumnIndices.[i]] <- actual.Values.[i]

            actual2D

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            Expect.equal actual2D.[i, j] expected.[i, j] "Elements of matrices should be equal."

let testCases =
    let q = context.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)

    let setSizeForMultFun mMult =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            // mMult (if wgSize = 1 then 2 else wgSize) q
            mMult 128 q

    let makeTest (context: ClContext) generator size mFormat times plus qTimes qPlus zero =
        let mtx1, mtx2, baseMtx1, baseMtx2 =
            getMatricesToMultiply generator size ((=) zero) mFormat

        match mtx1, mtx2 with
        | MatrixCOO mtx1, MatrixCOO mtx2 ->
            if mtx1.Values.Length > 0 && mtx2.Values.Length > 0 then
                use clRows1 = context.CreateClArray mtx1.Rows
                use clColumns1 = context.CreateClArray mtx1.Columns
                use clValues1 = context.CreateClArray mtx1.Values

                let m1 =
                    { Backend.COOMatrix.RowCount = mtx1.RowCount
                      ColumnCount = mtx1.ColumnCount
                      Rows = clRows1
                      Columns = clColumns1
                      Values = clValues1 }

                use clRows2 = context.CreateClArray mtx2.Rows
                use clColumns2 = context.CreateClArray mtx2.Columns
                use clValues2 = context.CreateClArray mtx2.Values

                let m2 =
                    { Backend.COOMatrix.RowCount = mtx2.RowCount
                      ColumnCount = mtx2.ColumnCount
                      Rows = clRows2
                      Columns = clColumns2
                      Values = clValues2 }

                let getMultFun =
                    COOMatrix.spgemm qTimes qPlus context |> setSizeForMultFun

                let mult = getMultFun mtx1.Values

                let actual =
                    let res: Backend.COOMatrix<'a> = mult m1 m2 zero
                    let actualRows = Array.zeroCreate res.Rows.Length
                    let actualColumns = Array.zeroCreate res.Columns.Length
                    let actualValues = Array.zeroCreate res.Values.Length

                    let _ =
                        q.Post(Msg.CreateToHostMsg(res.Rows, actualRows))

                    let _ =
                        q.Post(Msg.CreateToHostMsg(res.Columns, actualColumns))

                    let _ =
                        q.PostAndReply(fun ch -> Msg.CreateToHostMsg(res.Values, actualValues, ch))

                    q.Post(Msg.CreateFreeMsg<_>(res.Columns))
                    q.Post(Msg.CreateFreeMsg<_>(res.Rows))
                    q.Post(Msg.CreateFreeMsg<_>(res.Values))

                    { RowCount = res.RowCount
                      ColumnCount = res.ColumnCount
                      Rows = actualRows
                      Columns = actualColumns
                      Values = actualValues }

                logger.debug (
                    eventX "Actual is {actual}"
                    >> setField "actual" (sprintf "%A" actual)
                )

                checkResult times plus zero baseMtx1 baseMtx2 (MatrixCOO actual)

        | MatrixCSR mtx1, MatrixCSR mtx2 ->
            if mtx1.Values.Length > 0 && mtx2.Values.Length > 0 then
                use clRows1 = context.CreateClArray mtx1.RowPointers
                use clColumns1 = context.CreateClArray mtx1.ColumnIndices
                use clValues1 = context.CreateClArray mtx1.Values

                let m1 =
                    { Backend.CSRMatrix.RowCount = mtx1.RowCount
                      ColumnCount = mtx1.ColumnCount
                      RowPointers = clRows1
                      Columns = clColumns1
                      Values = clValues1 }

                use clRows2 = context.CreateClArray mtx2.RowPointers
                use clColumns2 = context.CreateClArray mtx2.ColumnIndices
                use clValues2 = context.CreateClArray mtx2.Values

                let m2 =
                    { Backend.CSRMatrix.RowCount = mtx2.RowCount
                      ColumnCount = mtx2.ColumnCount
                      RowPointers = clRows2
                      Columns = clColumns2
                      Values = clValues2 }

                let getMultFun =
                    CSRMatrix.spgemm qTimes qPlus context |> setSizeForMultFun

                let mult = getMultFun mtx1.Values

                let actual =
                    let res: Backend.CSRMatrix<'a> = mult m1 m2 zero
                    let actualRows = Array.zeroCreate res.RowPointers.Length
                    let actualColumns = Array.zeroCreate res.Columns.Length
                    let actualValues = Array.zeroCreate res.Values.Length

                    let _ =
                        q.Post(Msg.CreateToHostMsg(res.RowPointers, actualRows))

                    let _ =
                        q.Post(Msg.CreateToHostMsg(res.Columns, actualColumns))

                    let _ =
                        q.PostAndReply(fun ch -> Msg.CreateToHostMsg(res.Values, actualValues, ch))

                    q.Post(Msg.CreateFreeMsg<_>(res.Columns))
                    q.Post(Msg.CreateFreeMsg<_>(res.RowPointers))
                    q.Post(Msg.CreateFreeMsg<_>(res.Values))

                    { CSRMatrix.RowCount = res.RowCount
                      ColumnCount = res.ColumnCount
                      RowPointers = actualRows
                      ColumnIndices = actualColumns
                      Values = actualValues }

                logger.debug (
                    eventX "Actual is {actual}"
                    >> setField "actual" (sprintf "%A" actual)
                )

                checkResult times plus zero baseMtx1 baseMtx2 (MatrixCSR(actual))

        | _ -> failwith "No other types of matrices tested yet."

    [ testProperty "Correctness test on random int matrices CSR"
      <| (fun size -> makeTest context (PairOfMatricesOfCompatibleSize.IntType()) size CSR (*) (+) <@ (*) @> <@ (+) @> 0)

      testProperty "Correctness test on random bool matrices CSR"
      <| (fun size -> makeTest context (PairOfMatricesOfCompatibleSize.BoolType()) size CSR (&&) (||) <@ (&&) @> <@ (||) @> false)

      testProperty "Correctness test on random float matrices CSR"
      <| (fun size -> makeTest context (PairOfMatricesOfCompatibleSize.FloatType()) size CSR (*) (+) <@ (*) @> <@ (+) @> 0.0)

      testProperty "Correctness test on random byte matrices CSR"
      <| (fun size -> makeTest context (PairOfMatricesOfCompatibleSize.ByteType()) size CSR (*) (+) <@ (*) @> <@ (+) @> 0uy) ]

let tests =
    testCases |> testList "Backend.EwiseAdd tests"
