module Backend.Convert

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open OpenCL.Net

let logger = Log.create "Convert.Tests"

let config = defaultConfig
let wgSize = 32

let makeTestCSR context q toCOO isZero (array: 'a [,]) =
    let mtx = createMatrixFromArray2D CSR array isZero

    if mtx.NNZCount > 0 then
        let actual =
            let mCSR = mtx.ToBackend context
            let mCOO = toCOO q mCSR
            let res = Matrix.FromBackend q mCOO
            mCOO.Dispose q
            mCSR.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected = createMatrixFromArray2D COO array isZero

        "Matrices should be equal"
        |> Expect.equal actual expected

let makeTestCOO context q toCSR isZero (array: 'a [,]) =
    let mtx = createMatrixFromArray2D COO array isZero

    if mtx.NNZCount > 0 then
        let actual =
            let mCOO = mtx.ToBackend context
            let mCSR: Backend.Matrix<'a> = toCSR q mCOO
            let res = Matrix.FromBackend q mCSR
            mCOO.Dispose q
            mCSR.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected = createMatrixFromArray2D CSR array isZero

        "Matrices should be equal"
        |> Expect.equal actual expected

let testFixtures case =
    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A" datatype case.MatrixCase

    let filterFloat x =
        System.Double.IsNaN x
        || abs x < Accuracy.medium.absolute

    let context = case.ClContext.ClContext
    let q = case.ClContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    match case.MatrixCase with
    | COO ->
        [ let toCSR = Matrix.toCSR context wgSize

          makeTestCOO context q toCSR ((=) 0)
          |> testPropertyWithConfig config (getCorrectnessTestName "int")

          let toCSR = Matrix.toCSR context wgSize

          makeTestCOO context q toCSR filterFloat
          |> testPropertyWithConfig config (getCorrectnessTestName "float")

          let toCSR = Matrix.toCSR context wgSize

          makeTestCOO context q toCSR ((=) 0uy)
          |> testPropertyWithConfig config (getCorrectnessTestName "byte")

          let toCSR = Matrix.toCSR context wgSize

          makeTestCOO context q toCSR ((=) false)
          |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]
    | CSR ->
        [ let toCOO = Matrix.toCOO context wgSize

          makeTestCSR context q toCOO ((=) 0)
          |> testPropertyWithConfig config (getCorrectnessTestName "int")

          let toCOO = Matrix.toCOO context wgSize

          makeTestCSR context q toCOO filterFloat
          |> testPropertyWithConfig config (getCorrectnessTestName "float")

          let toCOO = Matrix.toCOO context wgSize

          makeTestCSR context q toCOO ((=) 0uy)
          |> testPropertyWithConfig config (getCorrectnessTestName "byte")

          let toCOO = Matrix.toCOO context wgSize

          makeTestCSR context q toCOO ((=) false)
          |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

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
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.MatrixCase)
    |> List.collect testFixtures
    |> testList "Convert tests"
