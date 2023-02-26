module GraphBLAS.FSharp.Tests.Backend.Vector.AssignByMask

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open Brahma.FSharp
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.assignByMask.Tests"

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let getCorrectnessTestName case datatype =
    $"Correctness on %s{datatype}, vector: %A{case.Format}"

let checkResult isZero isComplemented (actual: Vector<'a>) (vector: 'a []) (mask: 'a []) (value: 'a) =

    let expectedArray = Array.zeroCreate vector.Length

    let vector =
        Utils.createVectorFromArray Dense vector isZero
        |> Utils.vectorToDenseVector

    let mask =
        Utils.createVectorFromArray Dense mask isZero
        |> Utils.vectorToDenseVector

    for i in 0 .. vector.Length - 1 do
        expectedArray.[i] <-
            if isComplemented then
                match vector.[i], mask.[i] with
                | _, None -> Some value
                | _ -> vector.[i]
            else
                match vector.[i], mask.[i] with
                | _, Some _ -> Some value
                | _ -> vector.[i]

    match actual with
    | Vector.Dense actual -> Expect.equal actual expectedArray "Arrays must be equals"
    | _ -> failwith "Vector format must be Dense."

let makeTest<'a when 'a: struct and 'a: equality>
    (isZero: 'a -> bool)
    (toDense: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    (fillVector: MailboxProcessor<Msg> -> AllocationFlag -> ClVector<'a> -> ClVector<'a> -> ClCell<'a> -> ClVector<'a>)
    isComplemented
    case
    (vector: 'a [], mask: 'a [], value: 'a)
    =

    let leftVector =
        Utils.createVectorFromArray case.Format vector isZero

    let maskVector =
        Utils.createVectorFromArray case.Format mask isZero

    if leftVector.NNZ > 0 && maskVector.NNZ > 0 then

        let q = case.TestContext.Queue
        let context = case.TestContext.ClContext

        let clLeftVector = leftVector.ToDevice context
        let clMaskVector = maskVector.ToDevice context

        try
            let clValue = context.CreateClCell<'a> value

            let clActual =
                fillVector q HostInterop clLeftVector clMaskVector clValue

            let cooClActual = toDense q HostInterop clActual

            let actual = cooClActual.ToHost q

            clLeftVector.Dispose q
            clMaskVector.Dispose q
            clActual.Dispose q
            cooClActual.Dispose q

            checkResult isZero isComplemented actual vector mask value
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let createTest case (isZero: 'a -> bool) isComplemented fill =
    let context = case.TestContext.ClContext
    let getCorrectnessTestName = getCorrectnessTestName case

    let fill = fill context Mask.assign wgSize

    let toCoo = Vector.toDense context wgSize

    case
    |> makeTest isZero toCoo fill isComplemented
    |> testPropertyWithConfig config (getCorrectnessTestName $"%A{typeof<'a>}")

let testFixtures case =
    let context = case.TestContext.ClContext

    let isComplemented = false

    [ createTest case ((=) 0) isComplemented Vector.assignByMask

      if Utils.isFloat64Available context.ClDevice then
          createTest case (Utils.floatIsEqual 0) isComplemented Vector.assignByMask

      createTest case (Utils.float32IsEqual 0.0f) isComplemented Vector.assignByMask
      createTest case ((=) 0uy) isComplemented Vector.assignByMask
      createTest case ((=) false) isComplemented Vector.assignByMask ]

let tests =
    operationGPUTests "Backend.Vector.assignByMask tests"
    <| testFixtures

let testFixturesComplemented case =
    let context = case.TestContext.ClContext

    let isComplemented = true

    [ createTest case ((=) 0) isComplemented Vector.assignByMaskComplemented

      if Utils.isFloat64Available context.ClDevice then
          createTest case (Utils.floatIsEqual 0) isComplemented Vector.assignByMaskComplemented

      createTest case (Utils.float32IsEqual 0.0f) isComplemented Vector.assignByMaskComplemented
      createTest case ((=) 0uy) isComplemented Vector.assignByMaskComplemented
      createTest case ((=) false) isComplemented Vector.assignByMaskComplemented ]

let complementedTests =
    operationGPUTests "Backend.Vector.assignByMaskComplemented tests"
    <| testFixturesComplemented
