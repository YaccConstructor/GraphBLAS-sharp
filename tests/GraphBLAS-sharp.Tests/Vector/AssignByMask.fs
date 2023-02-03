module GraphBLAS.FSharp.Tests.Backend.Vector.AssignByMask

open System
open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open Brahma.FSharp
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.assignByMask.Tests"

let alwaysTrue _ = true

let notNane x = not <| Double.IsNaN x

let checkResult isZero isComplemented (actual: Vector<'a>) (vector: 'a []) (mask: 'a []) (value: 'a) =

    let expectedArray = Array.zeroCreate vector.Length

    let vector =
        createVectorFromArray Dense vector isZero
        |> vectorToDenseVector

    let mask =
        createVectorFromArray Dense mask isZero
        |> vectorToDenseVector

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
    isValueCompatible
    (toDense: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    (fillVector: MailboxProcessor<Msg> -> AllocationFlag -> ClVector<'a> -> ClVector<'a> -> ClCell<'a> -> ClVector<'a>)
    isComplemented
    case
    (vector: 'a [], mask: 'a [])
    (value: 'a)
    =

    let leftVector =
        createVectorFromArray case.Format vector isZero

    let maskVector =
        createVectorFromArray case.Format mask isZero

    if isValueCompatible value
       && leftVector.NNZ > 0
       && maskVector.NNZ > 0 then

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

let testFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, vector: %A{case.Format}"

    let wgSize = 32
    let context = case.TestContext.ClContext

    let isComplemented = false

    [ let intFill =
          Vector.assignByMask context Mask.assign wgSize

      let intToCoo = Vector.toDense context wgSize

      case
      |> makeTest ((=) 0) alwaysTrue intToCoo intFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatFill =
          Vector.assignByMask context Mask.assign wgSize

      let floatToCoo = Vector.toDense context wgSize

      case
      |> makeTest (floatIsEqual 0.0) notNane floatToCoo floatFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteFill =
          Vector.assignByMask context Mask.assign wgSize

      let byteToCoo = Vector.toDense context wgSize

      case
      |> makeTest ((=) 0uy) alwaysTrue byteToCoo byteFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let boolFill =
          Vector.assignByMask context Mask.assign wgSize

      let boolToCoo = Vector.toDense context wgSize

      case
      |> makeTest ((=) false) alwaysTrue boolToCoo boolFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    operationGPUTests "Backend.Vector.assignByMask tests" testFixtures

let testFixturesComplemented case =
    let config = defaultConfig

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, vector: %A{case.Format}"

    let wgSize = 32
    let context = case.TestContext.ClContext

    let isComplemented = true

    [ let intFill =
          Vector.assignByMaskComplemented context Mask.assign wgSize

      let intToCoo = Vector.toDense context wgSize

      case
      |> makeTest ((=) 0) alwaysTrue intToCoo intFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatFill =
          Vector.assignByMaskComplemented context Mask.assign wgSize

      let floatToCoo = Vector.toDense context wgSize

      case
      |> makeTest (floatIsEqual 0.0) notNane floatToCoo floatFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteFill =
          Vector.assignByMaskComplemented context Mask.assign wgSize

      let byteToCoo = Vector.toDense context wgSize

      case
      |> makeTest ((=) 0uy) alwaysTrue byteToCoo byteFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let boolFill =
          Vector.assignByMaskComplemented context Mask.assign wgSize

      let boolToCoo = Vector.toDense context wgSize

      case
      |> makeTest ((=) false) alwaysTrue boolToCoo boolFill isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let complementedTests =
    operationGPUTests "Backend.Vector.assignByMaskComplemented tests" testFixturesComplemented
