# bash completion for td commands

have td &&
_td()
{
    local cur prev words cword list
    _get_comp_words_by_ref cur prev words cword
    COMP_WORDBREAKS=${COMP_WORDBREAKS//:}
    list="`td help:all | awk '{print $1}' | grep '^[a-z]' | xargs`"

    # echo "cur=$cur, prev=$prev, words=$words, cword=$cword"

    if [[ "$prev" == "td" ]]; then
        if [[ "$cur" == "" ]]; then
            COMPREPLY=($list)
        else
            COMPREPLY=($(compgen -W "$list" -- "$cur"))
        fi
    fi
}
complete -F _td td

# Local variables:
# # mode: shell-script
# # sh-basic-offset: 4
# # sh-indent-comment: t
# # indent-tabs-mode: nil
# # End:
# # ex: ts=4 sw=4 et filetype=sh
