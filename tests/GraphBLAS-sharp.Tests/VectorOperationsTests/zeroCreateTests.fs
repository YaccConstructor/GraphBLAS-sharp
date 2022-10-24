module Backend.Vector.ZeroCreate

open Expecto
open Expecto.Logging

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.zeroCreate.Tests"

let checkResult size (actual: Vector<'a>) =

    match actual with
    | VectorDense vector ->

        Expect.equal actual.Size size "The size should be the same"

        Array.iter
        <| (fun item -> Expect.equal item None "values must be None")
        <| vector
    | VectorCOO vector ->
        Expect.equal actual.Size 0 "The size should be the 0"

        Expect.equal vector.Values [| Unchecked.defaultof<'a> |] "The values array must contain the default value"
        Expect.equal vector.Indices [| 0 |] "The index array must contain the 0"

let correctnessGenericTest<'a when 'a: struct and 'a: equality>
    (wgSize: int)
    (case: OperationCase<VectorFormat>)
    (vectorSize: int)
    =

    if vectorSize > 0 then
        let context = case.ClContext.ClContext
        let q = case.ClContext.Queue

        let clVector =
            Vector.zeroCreate<'a> context wgSize q vectorSize case.FormatCase

        let hostVector = clVector.ToHost q

        clVector.Dispose q

        checkResult vectorSize hostVector

let testFixtures (case: OperationCase<VectorFormat>) =

    let config = { defaultConfig with maxTest = 1}
    let wgSize = 32

    case
    |> correctnessGenericTest wgSize
    |> testPropertyWithConfig config (sprintf "Correctness on %A" case)

let tests =
     testCases
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.map testFixtures
    |> testList "Backend.Vector.zeroCreate tests"
