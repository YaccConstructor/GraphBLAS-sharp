module GraphBLAS.FSharp.Tests.Backend.Vector.Copy

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.copy.Tests"

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let checkResult (isEqual: 'a -> 'a -> bool) (actual: Vector<'a>) (expected: Vector<'a>) =
    Expect.equal actual.Size expected.Size "The size should be the same"

    match actual, expected with
    | Vector.Dense actual, Vector.Dense expected ->
        let isEqual left right =
            match left, right with
            | Some left, Some right -> isEqual left right
            | None, None -> true
            | _, _ -> false

        Utils.compareArrays isEqual actual expected "The values array must contain the default value"
    | Vector.Sparse actual, Vector.Sparse expected ->
        Utils.compareArrays isEqual actual.Values expected.Values "The values array must contain the default value"
        Utils.compareArrays (=) actual.Indices expected.Indices "The index array must contain the 0"
    | _ -> failwith "Copy format must be the same"

let correctnessGenericTest<'a when 'a: struct>
    isEqual
    zero
    (copy: MailboxProcessor<Brahma.FSharp.Msg> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (array: 'a [])
    =

    let expected =
        Utils.createVectorFromArray case.Format array (isEqual zero)

    if array.Length > 0 && expected.NNZ > 0 then

        let q = case.TestContext.Queue
        let context = case.TestContext.ClContext

        let clVector = expected.ToDevice context
        let clVectorCopy = copy q HostInterop clVector
        let actual = clVectorCopy.ToHost q

        clVector.Dispose q
        clVectorCopy.Dispose q

        checkResult isEqual actual expected

let createTest<'a when 'a: struct> case isEqual zero =
    let context = case.TestContext.ClContext

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, %A{case.Format}"

    let intCopy = Vector.copy context wgSize

    case
    |> correctnessGenericTest<'a> isEqual zero intCopy
    |> testPropertyWithConfig config (getCorrectnessTestName $"%A{typeof<'a>}")


let testFixtures (case: OperationCase<VectorFormat>) =
    let context = case.TestContext.ClContext

    [ createTest<int> case (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0

      createTest<float32> case Utils.float32IsEqual 0.0f
      createTest<bool> case (=) false
      createTest<byte> case (=) 0uy ]

let tests =
    operationGPUTests "Backend.Vector.copy tests" testFixtures
