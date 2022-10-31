module Backend.Elementwise

open System
open Brahma.FSharp.OpenCL.Shared
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open Microsoft.FSharp.Collections
open OpenCL.Net
open Backend.Common.StandardOperations

let logger = Log.create "Elementwise.Tests"

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
            if isEqual zero actual.Values.[i] then
                failwith "Resulting zeroes should be filtered."

            actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]
    | _ -> failwith "Resulting matrix should be converted to COO format."

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            Expect.isTrue
                (isEqual actual2D.[i, j] expected2D.[i, j])
                $"Values should be the same. Actual is {actual2D.[i, j]}, expected {expected2D.[i, j]}."

let correctnessGenericTest
    zero
    op
    (addFun: MailboxProcessor<_> -> Backend.Matrix<'a> -> Backend.Matrix<'b> -> Backend.Matrix<'c>)
    toCOOFun
    (isEqual: 'a -> 'a -> bool)
    q
    (case: OperationCase<MatrixFormat>)
    (leftMatrix: 'a [,], rightMatrix: 'a [,])
    =

    let mtx1 =
        createMatrixFromArray2D case.FormatCase leftMatrix (isEqual zero)

    let mtx2 =
        createMatrixFromArray2D case.FormatCase rightMatrix (isEqual zero)

    if mtx1.NNZCount > 0 && mtx2.NNZCount > 0 then
        try
            let m1 = mtx1.ToBackend case.ClContext.ClContext
            let m2 = mtx2.ToBackend case.ClContext.ClContext

            let res = addFun q m1 m2

            m1.Dispose q
            m2.Dispose q

            let cooRes = toCOOFun q res
            let actual = Matrix.FromBackend q cooRes

            cooRes.Dispose q
            res.Dispose q

            logger.debug (
                eventX "Actual is {actual}"
                >> setField "actual" (sprintf "%A" actual)
            )

            checkResult isEqual op zero leftMatrix rightMatrix actual
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let testFixturesEWiseAdd case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolAdd =
          Matrix.elementwise context boolSum wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd = Matrix.elementwise context intSum wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.elementwise context floatSum wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.elementwise context byteSum wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseAddTests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixturesEWiseAdd
    |> testList "Backend.Matrix.EWiseAdd tests"

let testFixturesEWiseAddAtLeastOne case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolAdd =
          Matrix.elementwiseAtLeastOne context boolSumAtLeastOne wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.elementwiseAtLeastOne context intSumAtLeastOne wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.elementwiseAtLeastOne context floatSumAtLeastOne wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.elementwiseAtLeastOne context byteSumAtLeastOne wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseAddAtLeastOneTests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixturesEWiseAddAtLeastOne
    |> testList "Backend.Matrix.EWiseAddAtLeastOne tests"

let testFixturesEWiseAddAtLeastOneToCOO case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolAdd =
          Matrix.elementwiseAtLeastOneToCOO context boolSumAtLeastOne wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.elementwiseAtLeastOneToCOO context intSumAtLeastOne wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.elementwiseAtLeastOneToCOO context floatSumAtLeastOne wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.elementwiseAtLeastOneToCOO context byteSumAtLeastOne wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseAddAtLeastOneToCOOTests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixturesEWiseAddAtLeastOneToCOO
    |> testList "Backend.Matrix.EWiseAddAtLeastOneToCOO tests"

let testFixturesEWiseMulAtLeastOne case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolMul =
          Matrix.elementwiseAtLeastOne context boolMulAtLeastOne wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (&&) boolMul boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.elementwiseAtLeastOne context intMulAtLeastOne wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (*) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.elementwiseAtLeastOne context floatMulAtLeastOne wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (*) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.elementwiseAtLeastOne context byteMulAtLeastOne wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (*) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseMulAtLeastOneTests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixturesEWiseMulAtLeastOne
    |> testList "Backend.Matrix.eWiseMulAtLeastOne tests"
