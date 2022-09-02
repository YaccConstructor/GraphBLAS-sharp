module BackendTests.EwiseAdd

open System
open Brahma.FSharp.OpenCL.Shared
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net
open GraphBLAS.FSharp.Backend.Common.StandardOperations

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
    (addFun: MailboxProcessor<_> -> ClMatrix<'a> -> ClMatrix<'a> -> ClMatrix<'a>)
    (toCOOFun: MailboxProcessor<Msg> -> ClMatrix<_> -> ClMatrix<_>)
    (isEqual: 'a -> 'a -> bool)
    q
    (case: OperationCase)
    (leftMatrix: 'a [,], rightMatrix: 'a [,])
    =

    let mtx1 = createMatrixFromArray2D case.MatrixCase leftMatrix (isEqual zero)
    let mtx2 = createMatrixFromArray2D case.MatrixCase rightMatrix (isEqual zero)

    if mtx1.NNZCount > 0 && mtx2.NNZCount > 0 then
        let m1 = mtx1.ToDevice case.ClContext.ClContext
        let m2 = mtx2.ToDevice case.ClContext.ClContext

        let res = addFun q m1 m2

        m1.Dispose q
        m2.Dispose q

        let cooRes = toCOOFun q res
        let actual = cooRes.ToHost q

        cooRes.Dispose q
        res.Dispose q

        logger.debug (eventX "Actual is {actual}" >> setField "actual" (sprintf "%A" actual))

        checkResult isEqual op zero leftMatrix rightMatrix actual

let testFixturesEWiseAdd case =
    [
        let config = defaultConfig
        let wgSize = 256

        let getCorrectnessTestName datatype = sprintf "Correctness on %s, %A" datatype case

        let context = case.ClContext.ClContext
        let q = case.ClContext.Queue

        let boolAdd = Matrix.eWiseAdd context boolSum wgSize
        let boolToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "bool")

        let intAdd = Matrix.eWiseAdd context intSum wgSize
        let intToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "int")

        let floatAdd = Matrix.eWiseAdd context floatSum wgSize
        let floatToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
        |> testPropertyWithConfig config (getCorrectnessTestName "float")

        let byteAdd = Matrix.eWiseAdd context byteSum wgSize
        let byteToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "byte")
    ]

let tests =
    testCases
    |> List.filter (fun case ->
        let mutable e = ErrorCode.Unknown
        let device = case.ClContext.ClContext.ClDevice.Device

        let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

        deviceType = DeviceType.Gpu
    )
    |> List.collect testFixturesEWiseAdd
    |> testList "Backend.Matrix.eWiseAdd tests"

let testFixturesEWiseAddAtLeastOne case =
    [
        let config = defaultConfig
        let wgSize = 256

        let getCorrectnessTestName datatype = sprintf "Correctness on %s, %A" datatype case

        let context = case.ClContext.ClContext
        let q = case.ClContext.Queue

        let boolAdd = Matrix.eWiseAddAtLeastOne context boolSumAtLeastOne wgSize
        let boolToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "bool")

        let intAdd = Matrix.eWiseAddAtLeastOne context intSumAtLeastOne wgSize
        let intToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "int")

        let floatAdd = Matrix.eWiseAddAtLeastOne context floatSumAtLeastOne wgSize
        let floatToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
        |> testPropertyWithConfig config (getCorrectnessTestName "float")

        let byteAdd = Matrix.eWiseAddAtLeastOne context byteSumAtLeastOne wgSize
        let byteToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "byte")
    ]

let tests2 =
    testCases
    |> List.filter (fun case ->
        let mutable e = ErrorCode.Unknown
        let device = case.ClContext.ClContext.ClDevice.Device

        let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

        deviceType = DeviceType.Gpu
    )
    |> List.collect testFixturesEWiseAddAtLeastOne
    |> testList "Backend.Matrix.eWiseAddAtLeastOne tests"

let testFixturesEWiseMulAtLeastOne case =
    [
        let config = defaultConfig
        let wgSize = 256

        let getCorrectnessTestName datatype = sprintf "Correctness on %s, %A" datatype case

        let context = case.ClContext.ClContext
        let q = case.ClContext.Queue

        let boolMul = Matrix.eWiseAddAtLeastOne context boolMulAtLeastOne wgSize
        let boolToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest false (&&) boolMul boolToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "bool")

        let intAdd = Matrix.eWiseAddAtLeastOne context intMulAtLeastOne wgSize
        let intToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0 (*) intAdd intToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "int")

        let floatAdd = Matrix.eWiseAddAtLeastOne context floatMulAtLeastOne wgSize
        let floatToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0.0 (*) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
        |> testPropertyWithConfig config (getCorrectnessTestName "float")

        let byteAdd = Matrix.eWiseAddAtLeastOne context byteMulAtLeastOne wgSize
        let byteToCOO = Matrix.toCOO context wgSize

        case
        |> correctnessGenericTest 0uy (*) byteAdd byteToCOO (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "byte")
    ]

let tests3 =
    testCases
    |> List.filter (fun case ->
        let mutable e = ErrorCode.Unknown
        let device = case.ClContext.ClContext.ClDevice.Device

        let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

        deviceType = DeviceType.Gpu
    )
    |> List.collect testFixturesEWiseMulAtLeastOne
    |> testList "Backend.Matrix.eWiseMulAtLeastOne tests"
