module BackendTests.EwiseAddBatched


open System
open Brahma.FSharp.OpenCL.Shared
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open OpenCL.Net
open GraphBLAS.FSharp.Backend.Common
open FSharp.Quotations

let logger = Log.create "EwiseAddBatched.Tests"

let checkResult isEqual op zero (arrays: 'a[,][]) (actual: COOMatrix<'a>) =
    let rows = Array2D.length1 arrays.[0]
    let columns = Array2D.length2 arrays.[0]
    Expect.equal columns actual.ColumnCount "The number of columns should be the same."
    Expect.equal rows actual.RowCount "The number of rows should be the same."

    let expected2D = Array2D.create rows columns zero

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            expected2D.[i, j] <- Array.reduce op (arrays |> Array.map (fun arr -> arr.[i, j]))

    let actual2D = Array2D.create rows columns zero

    for i in 0 .. actual.Rows.Length - 1 do
        if isEqual zero actual.Values.[i] then
            failwith "Resulting zeroes should be filtered."

        actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            Expect.isTrue
                (isEqual actual2D.[i, j] expected2D.[i, j])
                $"Values should be the same. Actual is {actual2D.[i, j]}, expected {expected2D.[i, j]}."

// TODO Add CSR
let correctnessGenericTest2
    (zero: 'a)
    (op: 'a -> 'a -> 'a)
    (addFun: MailboxProcessor<Msg> -> ClCooMatrix<'a>[] -> ClCooMatrix<'a>)
    (isEqual: 'a -> 'a -> bool)
    (q: MailboxProcessor<_>)
    (case: Utils.OperationCase)
    (arrays: 'a[,][]) =

    let matrices = Array.init arrays.Length <| fun i ->
        Utils.createMatrixFromArray2D case.MatrixCase arrays.[i] (isEqual zero)

    if matrices |> Array.forall (fun m -> m.NNZCount > 0) then
        logger.debug (
            eventX "{n} {x} {y}"
            >> setField "n" (sprintf "%A" arrays.Length)
            >> setField "x" (sprintf "%A" <| Array2D.length1 arrays.[0])
            >> setField "y" (sprintf "%A" <| Array2D.length1 arrays.[0])
        )

        let deviceMatrices =
            matrices
            |> Array.map (fun m -> m.ToDevice case.ClContext.ClContext)
            |> Array.map (fun (ClMatrixCOO m) -> m)

        let res = addFun q deviceMatrices
        let actual = res.ToHost q

        deviceMatrices |> Array.iter (fun m -> m.Dispose q)
        res.Dispose q

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" $"%A{actual}"
        )
        checkResult isEqual op zero arrays actual

let testFixturesEWiseAddBatched (case: Utils.OperationCase) =
    [
        let config = Utils.defaultConfig
        let wgSize = 256

        let getCorrectnessTestName datatype = $"Correctness on %s{datatype}, %A{case}"

        let context = case.ClContext.ClContext
        let q = case.ClContext.Queue

        let intAdd = EWiseAddBatched.eWiseAddBatched context StandardOperations.intSum wgSize

        case
        |> correctnessGenericTest2 0 (+) intAdd (=) q
        |> testPropertyWithConfig config (getCorrectnessTestName "int")
    ]

let tests =
    Utils.testCases
    |> List.filter (fun case ->
        let mutable e = ErrorCode.Unknown
        let device = case.ClContext.ClContext.ClDevice.Device
        let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

        deviceType = DeviceType.Gpu &&
        case.MatrixCase = COO
    )
    |> List.collect testFixturesEWiseAddBatched
    |> testList "Backend.Matrix.eWiseAddBatched tests"
