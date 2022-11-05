module Backend.Vector.Convert

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend
open OpenCL.Net

let logger =
    Log.create "Backend.Vector.Convert.Tests"

let config = defaultConfig
let wgSize = 32

let makeTestDense isZero context q (toCOO: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>) (array: 'a []) =
    if array.Length > 0 then
        let vector =
            createVectorFromArray VectorFormat.Dense array isZero

        let actual =
            let clDenseVector = vector.ToDevice context
            let clCooVector = toCOO q clDenseVector
            let result = clCooVector.ToHost q
            clCooVector.Dispose q
            clDenseVector.Dispose q
            result

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected =
            createVectorFromArray VectorFormat.Sparse array isZero

        Expect.equal actual expected "Vectors must be the same"

let testFixtures case =
    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A" datatype case.Format

    let filterFloat x =
        System.Double.IsNaN x
        || abs x < Accuracy.medium.absolute

    let context = case.ClContext.ClContext
    let q = case.ClContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ let toCoo = Vector.toCoo context wgSize

      makeTestDense ((=) 0) context q toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let toCoo = Vector.toCoo context wgSize

      makeTestDense filterFloat context q toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let toCoo = Vector.toCoo context wgSize

      makeTestDense ((=) 0uy) context q toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let toCoo = Vector.toCoo context wgSize

      makeTestDense ((=) false) context q toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    getTestFromFixtures<VectorFormat> testFixtures "Backend.Vector.Convert tests"
