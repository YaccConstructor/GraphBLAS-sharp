module Backend.COOMatrix.EwiseAdd

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend

type TestCOOMatrix<'elem when 'elem: struct> =
    { OriginalMatrix: option<option<'elem> [,]>
      RowCount: int
      ColumnCount: int
      Rows: array<int>
      Columns: array<int>
      Values: array<'elem> }


let logger = Log.create "COOMatrix.EwiseAdd.Tests"

let genCOOMatricesToAdd (_base: 'a [,]) i1 i2 =
    let m1 =
        Array2D.create (_base.GetLength 0) (_base.GetLength 1) None

    let m2 =
        Array2D.create (_base.GetLength 0) (_base.GetLength 1) None

    for i in 0 .. _base.GetLength 0 - 1 do
        for j in 0 .. _base.GetLength 1 - 1 do
            if (i + j) % (abs i1 % 20 + 1) <> 0 then
                m1.[i, j] <- Some _base.[i, j]

            if (i + j) % (abs i2 % 20 + 1) <> 0 then
                m2.[i, j] <- Some _base.[i, j]

    let getCOOMatrix (m: option<'a> [,]) =
        let rows = new ResizeArray<_>()
        let columns = new ResizeArray<_>()
        let values = new ResizeArray<'a>()

        m
        |> Array2D.iteri
            (fun i j v ->
                match v with
                | Some x ->
                    rows.Add i
                    columns.Add j
                    values.Add x
                | None -> ())

        { OriginalMatrix = Some m
          RowCount = _base.GetLength 0
          ColumnCount = _base.GetLength 1
          Rows = rows.ToArray()
          Columns = columns.ToArray()
          Values = values.ToArray() }

    getCOOMatrix m1, getCOOMatrix m2

let context =
    let deviceType = ClDeviceType.Default
    let platformName = ClPlatform.Any
    ClContext(platformName, deviceType)

let checkResult op zero (mtx1: TestCOOMatrix<'a>) (mtx2: TestCOOMatrix<'a>) (actual: TestCOOMatrix<'a>) =
    Expect.equal mtx1.ColumnCount actual.ColumnCount "The number of columns should be the same."
    Expect.equal mtx1.RowCount actual.RowCount "The number of rows should be the same."

    let expected =
        let rows = mtx1.OriginalMatrix.Value.GetLength 0
        let columns = mtx1.OriginalMatrix.Value.GetLength 1
        let expected = Array2D.create rows columns None

        for i in 0 .. rows - 1 do
            for j in 0 .. columns - 1 do
                match mtx1.OriginalMatrix.Value.[i, j], mtx2.OriginalMatrix.Value.[i, j] with
                | Some x, Some y -> expected.[i, j] <- Some(op x y)
                | None, None -> expected.[i, j] <- None
                | Some x, None
                | None, Some x -> expected.[i, j] <- Some(op x zero)

        expected

    let actual2D =
        let actual2D =
            Array2D.create actual.RowCount actual.ColumnCount None

        for i in 0 .. actual.Rows.Length - 1 do
            actual2D.[actual.Rows.[i], actual.Columns.[i]] <- Some actual.Values.[i]

        actual2D

    for i in 0 .. (mtx1.OriginalMatrix.Value.GetLength 0) - 1 do
        for j in 0 .. (mtx1.OriginalMatrix.Value.GetLength 1) - 1 do
            Expect.equal actual2D.[i, j] expected.[i, j] "Elements of matrices should be equals."


let testCases =
    let q = context.Provider.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)

    let getMAddFun mAdd =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            mAdd (if wgSize = 1 then 2 else wgSize) q

    let makeTest getReplicateFun op zero (mtx1: TestCOOMatrix<'a>) (mtx2: TestCOOMatrix<'a>) =
        if mtx1.Values.Length > 0 && mtx2.Values.Length > 0 then
            use clRows1 = context.CreateClArray mtx1.Rows
            use clColumns1 = context.CreateClArray mtx1.Columns
            use clValues1 = context.CreateClArray mtx1.Values

            let m1 =
                { COOMatrix.RowCount = mtx1.RowCount
                  ColumnCount = mtx1.ColumnCount
                  Rows = clRows1
                  Columns = clColumns1
                  Values = clValues1 }

            use clRows2 = context.CreateClArray mtx2.Rows
            use clColumns2 = context.CreateClArray mtx2.Columns
            use clValues2 = context.CreateClArray mtx2.Values

            let m2 =
                { COOMatrix.RowCount = mtx2.RowCount
                  ColumnCount = mtx2.ColumnCount
                  Rows = clRows2
                  Columns = clColumns2
                  Values = clValues2 }

            let replicate = getReplicateFun mtx1.Values

            let actual =
                let res: COOMatrix<'a> = replicate m1 m2
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

                { OriginalMatrix = None
                  RowCount = res.RowCount
                  ColumnCount = res.ColumnCount
                  Rows = actualRows
                  Columns = actualColumns
                  Values = actualValues }

            logger.debug (
                eventX "Actual is {actual}"
                >> setField "actual" (sprintf "%A" actual)
            )

            checkResult op zero mtx1 mtx2 actual

    [ testProperty "Correctness test on random int arrays"
      <| (let replicate = COOMatrix.eWiseAdd context <@ (+) @>
          let getReplicateFun = getMAddFun replicate

          fun (array: int [,]) i1 i2 ->
              let m1, m2 = genCOOMatricesToAdd array i1 i2
              makeTest getReplicateFun (+) 0 m1 m2)

    (*testProperty "Correctness test on random bool arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<bool>) -> makeTest getReplicateFun array)

      testProperty "Correctness test on random float arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<float>) -> makeTest getReplicateFun array)

      testProperty "Correctness test on random byte arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<byte>) -> makeTest getReplicateFun array)*)

     ]

let tests =
    testCases |> testList "COOMatrix.EwiseAdd tests"
