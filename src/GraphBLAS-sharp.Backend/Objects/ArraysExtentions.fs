namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Objects.MailboxProcessorExtensions

module ArraysExtensions =
    type ClArray<'a> with
        member this.FreeAndWait(q: MailboxProcessor<Msg>) =
            q.Post(Msg.CreateFreeMsg this)
            finish q

        member this.ToHost(q: MailboxProcessor<Msg>) =
            let dst = Array.zeroCreate this.Length
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(this, dst, ch))

        member this.Free(q: MailboxProcessor<_>) = q.Post <| Msg.CreateFreeMsg this

        member this.ToHostAndFree(q: MailboxProcessor<_>) =
            let result = this.ToHost q
            this.Free q

            result

        member this.Size = this.Length

    type 'a ``[]`` with
        member this.Size = this.Length

        member this.ToDevice(context: ClContext) = context.CreateClArray this

    let DenseVectorToString (array: 'a []) =
        [ sprintf "Dense Vector\n"
          sprintf "Size:    %i \n" array.Length
          sprintf "Values:  %A \n" array ]
        |> String.concat ""

    let DenseVectorFromArray (array: 'a [], isZero: 'a -> bool) =
        array
        |> Array.map (fun v -> if isZero v then None else Some v)
