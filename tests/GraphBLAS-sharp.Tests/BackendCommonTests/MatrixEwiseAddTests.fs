module Backend.EwiseAdd

open System
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net
open StandardOperations

let logger = Log.create "EwiseAdd.Tests"

let checkResult isEqual op zero (baseMtx1: 'a [,]) (baseMtx2: 'a [,]) (actual: Matrix<'a>) =
    let rows = Array2D.length1 baseMtx1
    let columns = Array2D.length2 baseMtx1
    Expect.equal columns actual.ColumnCount "The number of columns should be the same."
    Expect.equal rows actual.RowCount "The number of rows should be the same."

    let expected2D = Array2D.create rows columns zero

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            expected2D.[i, j] <- op baseMtx1.[i, j] baseMtx2.[i, j]

    let actual2D = Array2D.create rows columns zero

    match actual with
    | MatrixCOO actual ->
        for i in 0 .. actual.Rows.Length - 1 do
            if isEqual zero actual.Values.[i] then failwith "Resulting zeroes should be filtered."
            actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]
    | _ -> failwith "Resulting matrix should be converted to COO format."

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            Expect.isTrue (isEqual actual2D.[i, j] expected2D.[i, j]) $"Values should be the same. Actual is {actual2D.[i, j]}, expected {expected2D.[i, j]}."

let correctnessGenericTest
    zero
    op
    (addFun: MailboxProcessor<Msg> -> Backend.Matrix<'a> -> Backend.Matrix<'b> -> Backend.Matrix<'c>)
    toCOOFun
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (leftMatrix: 'a [,], rightMatrix: 'a [,])
    =
    let q = case.ClContext.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)

    let mtx1 =
        createMatrixFromArray2D case.MatrixCase leftMatrix (isEqual zero)

    let mtx2 =
        createMatrixFromArray2D case.MatrixCase rightMatrix (isEqual zero)

    if mtx1.NNZCount > 0 && mtx2.NNZCount > 0 then
        let m1 = mtx1.ToBackend case.ClContext
        let m2 = mtx2.ToBackend case.ClContext

        let res = addFun q m1 m2

        m1.Dispose()
        m2.Dispose()

        let cooRes = toCOOFun q res
        let actual = Matrix.FromBackend q cooRes

        cooRes.Dispose()
        res.Dispose()

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        checkResult isEqual op zero leftMatrix rightMatrix actual

let testFixtures case =
    [ let config = defaultConfig
      let wgSize = 128
      //Test name on multiple devices can be duplicated due to the ClContext.toString
      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A, %A" datatype case (System.Random().Next())

      let boolAdd =
          Matrix.eWiseAdd case.ClContext boolSum wgSize

      let boolToCOO = Matrix.toCOO case.ClContext wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.eWiseAdd case.ClContext intSum wgSize

      let intToCOO = Matrix.toCOO case.ClContext wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.eWiseAdd case.ClContext floatSum wgSize

      let floatToCOO = Matrix.toCOO case.ClContext wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute)
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.eWiseAdd case.ClContext byteSum  wgSize

      let byteToCOO = Matrix.toCOO case.ClContext wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")
      ]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu
            )
    |> List.collect testFixtures
    |> testList "Backend.Matrix.eWiseAdd tests"
