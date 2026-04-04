'use client'

import * as React from 'react'
import { useUserStore, useKBStore } from '@/stores'
import { createClient } from '@/lib/supabase/client'

interface AuthProviderProps {
  userId: string
  email: string
  children: React.ReactNode
}

export function AuthProvider({ userId, email, children }: AuthProviderProps) {
  const setUser = useUserStore((s) => s.setUser)
  const setAccessToken = useUserStore((s) => s.setAccessToken)
  const fetchKBs = useKBStore((s) => s.fetchKBs)

  React.useEffect(() => {
    const supabase = createClient()
    supabase.auth.getSession().then(({ data: { session } }) => {
      if (session) {
        setUser({ id: userId, email })
        setAccessToken(session.access_token)
        fetchKBs()
      }
    })

    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      if (session) {
        useUserStore.getState().setAccessToken(session.access_token)
      }
    })

    return () => subscription.unsubscribe()
  }, [userId, email, setUser, setAccessToken, fetchKBs])

  return <>{children}</>
}
