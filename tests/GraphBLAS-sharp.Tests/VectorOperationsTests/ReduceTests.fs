module Backend.Vector.Reduce

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open Brahma.FSharp
open FSharp.Quotations

let logger = Log.create "Vector.Complemented.Tests"

let zeroFilter array isZero =
    Array.filter
    <| (fun item -> not <| isZero item)
    <| array

let checkResult
    zero
    op
    (actual: 'a)
    (vector: 'a [])
    =

    let expected =
        Array.fold op zero vector

    "Results should be the same"
    |> Expect.equal actual expected

let correctnessGenericTest
    isEqual
    zero
    op
    opQ
    (reduce: Expr<'a -> 'a -> 'a> -> 'a ->  MailboxProcessor<_> -> ClVector<'a> -> ClCell<'a>)
    filter
    case
    (array: 'a [])
    =

    let array = filter array

    let filteredArray =
        zeroFilter array (isEqual zero)

    if filteredArray.Length > 0 then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let vector = createVectorFromArray case.FormatCase array (isEqual zero)

        let clVector = vector.ToDevice context

        let resultCell = reduce opQ zero q clVector

        let result = Array.zeroCreate 1

        let result =
            let res =
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(resultCell, result, ch))

            q.Post(Msg.CreateFreeMsg<_>(resultCell))

            res[0]

        checkResult zero op result array

let addTestFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.FormatCase}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let filterFloats =
        Array.filter (System.Double.IsNaN >> not)

    [ let intReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0 (+) <@ (+) @> intReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0uy (+) <@ (+) @> byteReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let intMaxReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0 max <@ max @> intMaxReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "int max")

      let floatMaxReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0.0 max <@ max @> floatMaxReduce filterFloats
      |> testPropertyWithConfig config (getCorrectnessTestName "float max")

      let byteMaxReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0uy max <@ max @> byteMaxReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte max")

      let intMinReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Int32.MaxValue min <@ min @> intMinReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "int min")

      let floatMinReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Double.MaxValue min <@ min @> floatMinReduce filterFloats
      |> testPropertyWithConfig config (getCorrectnessTestName "float min")

      let byteMinReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Byte.MaxValue min <@ min @> byteMinReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte min")

      let boolOrReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) false (||) <@ (||) @> boolOrReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool or")

      let boolAndReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) true (&&) <@ (&&) @> boolAndReduce id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool and") ]

let tests =
    testCases<VectorFormat>
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect addTestFixtures
    |> testList "Backend.Vector.Reduce tests"
