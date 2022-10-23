namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp

module ArraysExtensions =

    type ClArray<'a> with
        member this.Dispose(q: MailboxProcessor<Msg>) =
            q.PostAndReply(fun _ -> Msg.CreateFreeMsg this)

        member this.ToHost(q: MailboxProcessor<Msg>) =
            let dst = Array.zeroCreate this.Length
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(this, dst, ch))

        member this.Size = this.Length

    type 'a ``[]`` with
        member this.Size = this.Length

        member this.ToString() =
            [ sprintf "Dense Vector\n"
              sprintf "Size:    %i \n" this.Length
              sprintf "Values:  %A \n" this ]
            |> String.concat ""

        member this.ToDevice(context: ClContext) = context.CreateClArray this

        static member FromArray(array: 'a [], isZero: 'a -> bool) =
            array
            |> Array.map (fun v -> if isZero v then None else Some v)
