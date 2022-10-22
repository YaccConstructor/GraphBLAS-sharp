module Backend.Vector.ZeroCreate

open Expecto
open Expecto.Logging

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.zeroCreate.Tests"

let checkResult size (actual: Vector<'a>) =

    Expect.equal actual.Size size "The size should be the same."

    match actual with
    | VectorDense vector ->
        let actualValues = vector.Values

        for i in 0..actual.Size - 1 do
            Expect.equal  actualValues[i] None "" //TODO()
    | VectorCOO vector ->
        let actualValues = vector.Values
        let actualIndices = vector.Indices

        Expect.equal actualValues [||] "" //TODO()
        Expect.equal actualIndices [||] "" //TODO()

let testFixtures (case: OperationCase) =
    let config = defaultConfig

    let wgSize = 32

    let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

    let context = case.ClContext.ClContext
    let q = case.ClContext.Queue

    let zeroCreate = Vector.zeroCreate context wgSize

    ()

