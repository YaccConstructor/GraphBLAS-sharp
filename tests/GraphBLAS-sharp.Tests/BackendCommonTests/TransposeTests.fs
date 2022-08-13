module Backend.Transpose

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net

let logger = Log.create "Transpose.Tests"

let context = defaultContext.ClContext
let config = defaultConfig
let wgSize = 128

let checkResult areEqual zero actual (expected2D: 'a[,]) =
    let actual2D =
        match actual with
        | MatrixCOO actual ->
            let actual2D = Array2D.create actual.RowCount actual.ColumnCount zero

            for i in 0 .. actual.Values.Length - 1 do
                "Resulting zeroes should be filtered."
                |> Expect.isFalse (areEqual zero actual.Values.[i])

                actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]

            actual2D
        | MatrixCSR actual ->
            let actual2D = Array2D.create actual.RowCount actual.ColumnCount zero

            for i in 0 .. actual.RowCount - 1 do
                for j in actual.RowPointers.[i] .. actual.RowPointers.[i + 1] - 1 do
                    "Resulting zeroes should be filtered."
                    |> Expect.isFalse (areEqual zero actual.Values.[j])

                    actual2D.[i, actual.ColumnIndices.[j]] <- actual.Values.[j]

            actual2D

    "The number of rows should be the same"
    |> Expect.equal (Array2D.length1 actual2D) (Array2D.length1 expected2D)

    "The number of columns should be the same"
    |> Expect.equal (Array2D.length2 actual2D) (Array2D.length2 expected2D)


    for i in 0 .. Array2D.length1 actual2D - 1 do
        for j in 0 .. Array2D.length2 actual2D - 1 do
            "Matrices should be equal"
            |> Expect.isTrue (areEqual actual2D.[i, j] expected2D.[i, j])

let makeTest q transposeFun areEqual zero case (array: 'a[,]) =
    let mtx = createMatrixFromArray2D case.MatrixCase array (areEqual zero)

    if mtx.NNZCount > 0 then
        let actual =
            let m = mtx.ToBackend context
            let mT = transposeFun q m
            let res = Matrix.FromBackend q mT
            m.Dispose q
            mT.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected2D =
            Array2D.create
                (Array2D.length2 array)
                (Array2D.length1 array)
                zero

        for i in 0 .. Array2D.length1 expected2D - 1 do
            for j in 0 .. Array2D.length2 expected2D - 1 do
                expected2D.[i, j] <- array.[j, i]

        checkResult areEqual zero actual expected2D

let testFixtures case =
    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A" datatype case.MatrixCase

    let areEqualFloat x y = System.Double.IsNaN x && System.Double.IsNaN y || x = y

    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [
        let transposeFun = Matrix.transpose context wgSize
        case
        |> makeTest q transposeFun (=) 0
        |> testPropertyWithConfig config (getCorrectnessTestName "int")

        let transposeFun = Matrix.transpose context wgSize
        case
        |> makeTest q transposeFun areEqualFloat 0.0
        |> testPropertyWithConfig config (getCorrectnessTestName "float")

        let transposeFun = Matrix.transpose context wgSize
        case
        |> makeTest q transposeFun (=) 0uy
        |> testPropertyWithConfig config (getCorrectnessTestName "byte")

        let transposeFun = Matrix.transpose context wgSize
        case
        |> makeTest q transposeFun (=) false
        |> testPropertyWithConfig config (getCorrectnessTestName "bool")
    ]

let tests =
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
    |> List.distinctBy (fun case -> case.MatrixCase)
    |> List.collect testFixtures
    |> testList "Transpose tests"
