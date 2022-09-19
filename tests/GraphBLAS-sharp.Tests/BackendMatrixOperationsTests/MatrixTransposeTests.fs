module BackendTests.Transpose

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net
open Brahma.FSharp

let logger = Log.create "Transpose.Tests"

let config = defaultConfig
let wgSize = 32

let checkResult areEqual zero actual (expected2D: 'a [,]) =
    match actual with
    | MatrixCOO actual ->
        let (MatrixCOO expected) = createMatrixFromArray2D MatrixFormat.COO expected2D (areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row arrays should be equal"
        |> compareArrays (=) actual.Rows expected.Rows

        "Column arrays should be equal"
        |> compareArrays (=) actual.Columns expected.Columns

        "Value arrays should be equal"
        |> compareArrays areEqual actual.Values expected.Values

    | MatrixCSR actual ->
        let (MatrixCSR expected) = createMatrixFromArray2D MatrixFormat.CSR expected2D (areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row pointer arrays should be equal"
        |> compareArrays (=) actual.RowPointers expected.RowPointers

        "Column arrays should be equal"
        |> compareArrays (=) actual.ColumnIndices expected.ColumnIndices

        "Value arrays should be equal"
        |> compareArrays areEqual actual.Values expected.Values

let makeTestRegular context q (transposeFun: MailboxProcessor<Msg> -> ClMatrix<'a> -> ClMatrix<'a>) areEqual zero case (array: 'a[,]) =
    let mtx = createMatrixFromArray2D case.MatrixCase array (areEqual zero)

    if mtx.NNZCount > 0 then
        let actual =
            let m = mtx.ToDevice context
            let mT = transposeFun q m
            let res = mT.ToHost q
            m.Dispose q
            mT.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" $"%A{actual}"
        )

        let expected2D =
            Array2D.create (Array2D.length2 array) (Array2D.length1 array) zero

        for i in 0 .. Array2D.length1 expected2D - 1 do
            for j in 0 .. Array2D.length2 expected2D - 1 do
                expected2D.[i, j] <- array.[j, i]

        checkResult areEqual zero actual expected2D

let makeTestTwiceTranspose context q (transposeFun: MailboxProcessor<Msg> -> ClMatrix<'a> -> ClMatrix<'a>) areEqual zero case (array: 'a[,]) =
    let mtx = createMatrixFromArray2D case.MatrixCase array (areEqual zero)

    if mtx.NNZCount > 0 then
        let actual =
            let m = mtx.ToDevice context
            let mT = transposeFun q m
            let mTT = transposeFun q mT
            let res = mTT.ToHost q
            m.Dispose q
            mT.Dispose q
            mTT.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" $"%A{actual}"
        )

        checkResult areEqual zero actual array

let testFixtures case =
    let getCorrectnessTestName datatype = $"Correctness on %s{datatype}, %A{case.MatrixCase}"

    let areEqualFloat x y =
        System.Double.IsNaN x && System.Double.IsNaN y
        || x = y

    let context = case.ClContext.ClContext
    let q = case.ClContext.Queue

    [
        let transposeFun = Matrix.transpose context wgSize

        case
        |> makeTestRegular context q transposeFun (=) 0
        |> testPropertyWithConfig config (getCorrectnessTestName "int")

        case
        |> makeTestTwiceTranspose context q transposeFun (=) 0
        |> testPropertyWithConfig config (getCorrectnessTestName "int (twice transpose)")

        let transposeFun = Matrix.transpose context wgSize

        case
        |> makeTestRegular context q transposeFun areEqualFloat 0.0
        |> testPropertyWithConfig config (getCorrectnessTestName "float")

        case
        |> makeTestTwiceTranspose context q transposeFun areEqualFloat 0.0
        |> testPropertyWithConfig config (getCorrectnessTestName "float (twice transpose)")

        let transposeFun = Matrix.transpose context wgSize

        case
        |> makeTestRegular context q transposeFun (=) 0uy
        |> testPropertyWithConfig config (getCorrectnessTestName "byte")

        case
        |> makeTestTwiceTranspose context q transposeFun (=) 0uy
        |> testPropertyWithConfig config (getCorrectnessTestName "byte (twice transpose)")

        let transposeFun = Matrix.transpose context wgSize

        case
        |> makeTestRegular context q transposeFun (=) false
        |> testPropertyWithConfig config (getCorrectnessTestName "bool")

        case
        |> makeTestTwiceTranspose context q transposeFun (=) false
        |> testPropertyWithConfig config (getCorrectnessTestName "bool (twice transpose)")
    ]

let tests =
    testCases
    |> List.filter (fun case ->
        let mutable e = ErrorCode.Unknown
        let device = case.ClContext.ClContext.ClDevice.Device
        let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<OpenCL.Net.DeviceType>()

        deviceType = DeviceType.Gpu
    )
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.MatrixCase)
    |> List.collect testFixtures
    |> testList "Transpose tests"
